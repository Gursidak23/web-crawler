"""Unit tests for URL normalization and domain extraction."""

from crawler.url_utils import host_of, normalize_url, registered_domain


def test_resolves_relative_against_base():
    assert normalize_url("/about", "http://example.com/dir/page") == "http://example.com/about"
    assert (
        normalize_url("sub/x.html", "http://example.com/dir/page")
        == "http://example.com/dir/sub/x.html"
    )


def test_drops_fragment():
    assert normalize_url("http://example.com/a#section") == "http://example.com/a"


def test_strips_tracking_params_but_keeps_real_ones():
    out = normalize_url("http://example.com/a?utm_source=news&id=5&fbclid=xyz")
    assert out is not None
    assert "utm_source" not in out
    assert "fbclid" not in out
    assert "id=5" in out


def test_lowercases_scheme_and_host():
    out = normalize_url("HTTP://Example.COM/Path")
    assert out is not None
    assert out.startswith("http://example.com/")
    assert out.endswith("/Path")  # path case is preserved


def test_rejects_non_http_schemes():
    assert normalize_url("mailto:a@b.com") is None
    assert normalize_url("javascript:void(0)") is None
    assert normalize_url("ftp://example.com/file") is None
    assert normalize_url("#anchor") is None
    assert normalize_url("") is None
    assert normalize_url(None) is None


def test_absolute_href_overrides_base():
    assert normalize_url("http://other.com/x", "http://example.com") == "http://other.com/x"


def test_registered_domain():
    assert registered_domain("http://www.bbc.co.uk/news") == "bbc.co.uk"
    assert registered_domain("https://example.com/a/b") == "example.com"
    assert registered_domain("http://deep.sub.example.com") == "example.com"


def test_host_of_includes_subdomain_and_lowercases():
    assert host_of("http://www.Example.com:8080/a") == "www.example.com"
