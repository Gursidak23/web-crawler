"""Unit tests for HTML parsing: title, link extraction, base href, dedup."""

from crawler.parser import extract_text, parse_page

HTML = """
<html>
  <head>
    <title>  Example Page  </title>
    <base href="/base/">
  </head>
  <body>
    <a href="page1.html">One</a>
    <a href="/abs">Two</a>
    <a href="http://other.com/x">External</a>
    <a href="mailto:a@b.com">Mail</a>
    <a href="page1.html">Duplicate</a>
    <a href="#frag">Fragment only</a>
  </body>
</html>
"""


def test_extracts_title_trimmed():
    page = parse_page("http://example.com/dir/page", HTML)
    assert page.title == "Example Page"


def test_resolves_links_with_base_href():
    page = parse_page("http://example.com/dir/page", HTML)
    urls = [link.url for link in page.links]
    assert "http://example.com/base/page1.html" in urls
    assert "http://example.com/abs" in urls
    assert "http://other.com/x" in urls


def test_skips_non_http_and_fragment_links():
    page = parse_page("http://example.com/dir/page", HTML)
    urls = [link.url for link in page.links]
    assert all("mailto" not in u for u in urls)
    assert all("#frag" not in u for u in urls)


def test_dedupes_repeated_links():
    page = parse_page("http://example.com/dir/page", HTML)
    urls = [link.url for link in page.links]
    assert urls.count("http://example.com/base/page1.html") == 1


def test_anchor_text_captured():
    page = parse_page("http://example.com/dir/page", HTML)
    by_url = {link.url: link.anchor for link in page.links}
    assert by_url["http://example.com/abs"] == "Two"


def test_extract_text_drops_scripts_and_styles():
    html = "<html><body><p>Hello</p><script>var x=1;</script><style>.a{}</style></body></html>"
    text = extract_text(html)
    assert "Hello" in text
    assert "var x" not in text
    assert ".a{}" not in text
