"""Authentication evidence, kept in memory and rendered without credentials."""

import html
import json
import re
import traceback
from dataclasses import dataclass, field
from http.cookies import CookieError, SimpleCookie
from html.parser import HTMLParser
from urllib.parse import quote, quote_plus, unquote


_SECRET_NAME = r"(?:cookie|authorization|password|passwd|token|access_token|refresh_token|id_token|ticket|code|state|session_state|session_?code|auth_?session_?id|tab_?id|execution|lt|nonce|csrf\w*)"
_ATTR = re.compile(r"""([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))""")


def _attributes(tag):
    return {
        match[1].lower(): html.unescape(
            next(v for v in match.groups()[1:] if v is not None)
        )
        for match in _ATTR.finditer(tag)
    }


@dataclass
class AuthDiagnostic:
    stage: str = "configuration"
    credential_sources: str = ""
    password_submitted: bool = False
    responses: list = field(default_factory=list)
    attempts: list = field(default_factory=list)
    secrets: set = field(default_factory=set, repr=False)

    def remember(self, value):
        if isinstance(value, str) and value:
            self.secrets.add(value)

    def remember_cookie(self, value):
        cookies = SimpleCookie()
        try:
            cookies.load(str(value))
        except CookieError:
            self.remember(str(value).split(";", 1)[0].partition("=")[2])
        for item in cookies.values():
            self.remember(item.value)

    def redact(self, text):
        text = str(text)
        # Learn dynamic values before formatting exception strings and URLs.
        for match in re.finditer(r"<input\b[^>]*>", text, re.I | re.S):
            attrs = _attributes(match[0])
            if re.fullmatch(_SECRET_NAME, attrs.get("name", ""), re.I):
                self.remember(attrs.get("value", ""))
        for match in re.finditer(
            rf"[?&](?:amp;)?{_SECRET_NAME}=([^&\s\"'<>]+)", text, re.I
        ):
            self.remember(match[1])
            self.remember(unquote(match[1]))

        def hide_input(match):
            tag = match[0]
            attrs = _attributes(tag)
            if attrs.get("type", "").lower() == "password" or re.fullmatch(
                _SECRET_NAME, attrs.get("name", ""), re.I
            ):
                return re.sub(
                    r"""(\bvalue\s*=\s*)(?:"[^"]*"|'[^']*'|[^\s>]+)""",
                    r'\1"[REDACTED]"',
                    tag,
                    flags=re.I | re.S,
                )
            return tag

        text = re.sub(r"<input\b[^>]*>", hide_input, text, flags=re.I | re.S)
        text = re.sub(rf"(?i)(\b{_SECRET_NAME}=)([^&\s\"'<>]*)", r"\1[REDACTED]", text)
        text = re.sub(
            rf"""(?i)(["']{_SECRET_NAME.replace('|code|', '|')}["']\s*:\s*)(["'])(.*?)\2""",
            r"\1\2[REDACTED]\2",
            text,
        )
        text = re.sub(
            r"(?im)^((?:set-cookie|cookie|authorization|proxy-authorization)\s*:\s*).*$",
            r"\1[REDACTED]",
            text,
        )
        for secret in sorted(self.secrets, key=len, reverse=True):
            # Short values are masked at their field; replacing every letter of a
            # one-character test password would destroy the rest of the evidence.
            if len(secret) >= 4:
                for variant in {
                    secret,
                    html.escape(secret, quote=True),
                    quote(secret, safe=""),
                    quote_plus(secret),
                    json.dumps(secret, ensure_ascii=False)[1:-1],
                    json.dumps(secret, ensure_ascii=True)[1:-1],
                }:
                    text = text.replace(variant, "[REDACTED]")
        return text

    def record(self, response, method, url):
        headers = dict(getattr(response, "headers", {}) or {})
        for key, value in headers.items():
            if key.lower() in {"set-cookie", "cookie"}:
                self.remember_cookie(value)
        # Keep the original text, including whitespace, rather than reserializing JSON.
        body = self.redact(getattr(response, "text", ""))
        self.responses.append(
            {
                "stage": self.stage,
                "method": method.upper(),
                "url": self.redact(getattr(response, "url", None) or url),
                "status": response.status_code,
                "headers": {
                    k: (
                        "[REDACTED]"
                        if k.lower()
                        in {
                            "set-cookie",
                            "cookie",
                            "authorization",
                            "proxy-authorization",
                        }
                        else self.redact(v)
                    )
                    for k, v in headers.items()
                },
                "body": body,
            }
        )

    def render(self):
        from . import __version__

        lines = [f"qzcli {__version__} | authentication stage: {self.stage}"]
        if self.credential_sources:
            lines.append(self.credential_sources)
        lines.append(f"password submitted: {self.password_submitted}")
        lines.extend(self.attempts)
        for i, r in enumerate(self.responses, 1):
            lines.append(
                f"\n--- response {i}: {r['stage']} | {r['method']} {r['url']} | HTTP {r['status']} ---"
            )
            lines.extend(f"{k}: {v}" for k, v in r["headers"].items())
            lines.append("")
            lines.append(r["body"])
        return self.redact("\n".join(lines))


def has_auth_diagnostic(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if getattr(exc, "diagnostic", None) is not None:
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def format_exception(exc):
    """Full causal traceback and response bodies; no locals or raw request bodies."""
    diagnostics = []
    seen = set()
    cursor = exc
    while cursor is not None and id(cursor) not in seen:
        seen.add(id(cursor))
        diag = getattr(cursor, "diagnostic", None)
        if diag is not None and all(diag is not other for other in diagnostics):
            diagnostics.append(diag)
        cursor = cursor.__cause__ or cursor.__context__
    rendered = "\n\n".join(d.render() for d in reversed(diagnostics))
    rendered += "\n\n" + "".join(
        traceback.TracebackException.from_exception(exc, capture_locals=False).format(
            chain=True
        )
    )
    for diag in diagnostics:
        rendered = diag.redact(rendered)
    # Also masks structured token fields in an exception without an auth response.
    return AuthDiagnostic().redact(rendered).lstrip()


class CasPage(HTMLParser):
    """Parse the submitted password form, including nested error markup."""

    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.forms = []
        self.current = None
        self.error_depth = 0
        self.error_text = []
        self.feed(text or "")

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.current = {"attrs": attrs, "inputs": {}, "errors": []}
            self.forms.append(self.current)
        if self.current is not None and tag == "input" and attrs.get("name"):
            self.current["inputs"][attrs["name"]] = attrs.get("value", "")
        if self.current is not None and "form-error" in attrs.get("class", "").split():
            self.error_depth = 1
            self.error_text = []
        elif self.error_depth and tag not in {
            "br",
            "input",
            "img",
            "hr",
            "meta",
            "link",
        }:
            self.error_depth += 1
        if self.error_depth and tag == "br":
            self.error_text.append(" ")

    def handle_endtag(self, tag):
        if self.error_depth and tag not in {"br", "input", "img", "hr", "meta", "link"}:
            self.error_depth -= 1
            if not self.error_depth and self.current is not None:
                self.current["errors"].append(
                    " ".join("".join(self.error_text).split())
                )
        if tag == "form":
            self.current = None

    def handle_data(self, data):
        if self.error_depth:
            self.error_text.append(data)

    @property
    def password_form(self):
        for form in self.forms:
            if form["attrs"].get("id") == "fm1" or "password" in form["inputs"]:
                return form
        return self.forms[0] if len(self.forms) == 1 else {"inputs": {}, "errors": []}

    @property
    def error(self):
        return "\n".join(x for x in self.password_form["errors"] if x)
