from __future__ import annotations


def render_string(source: str, *args, **kwargs) -> str:
        from jinja2 import Template
        return Template(source).render(*args, **kwargs)
