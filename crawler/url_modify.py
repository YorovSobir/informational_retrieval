from urllib.parse import urlparse
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
import urllib


def asciify_url(url, force_quote=False):

    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        # apparently not an url
        return url

    # idna-encode domain
    hostname = parts.hostname.encode('idna')

    # UTF8-quote the other parts. We check each part individually if
    # if needs to be quoted - that should catch some additional user
    # errors, say for example an umlaut in the username even though
    # the path *is* already quoted.
    def quote(s, safe):
        s = s or ''
        # Triggers on non-ascii characters - another option would be:
        #     urllib.quote(s.replace('%', '')) != s.replace('%', '')
        # which would trigger on all %-characters, e.g. "&".
        if s.encode('ascii', 'replace') != s or force_quote:
            return urllib.parse.quote(s.encode('utf8'), safe=safe)
        return s
    username = quote(parts.username, '')
    password = quote(parts.password, safe='')
    path = quote(parts.path, safe='/')
    query = quote(parts.query, safe='&=')

    # put everything back together
    netloc = hostname
    if username or password:
        netloc = '@' + netloc
        if password:
            netloc = ':' + password + netloc
        netloc = username + netloc
    if parts.port:
        netloc += ':' + str(parts.port)

    return urlunsplit([
        parts.scheme, netloc.decode(), path, query, parts.fragment])
