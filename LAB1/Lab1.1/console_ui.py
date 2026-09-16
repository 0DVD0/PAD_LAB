"""Afisare colorata, cu fallback daca Rich nu este instalat."""

from typing import Any

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except ImportError:
    Console = None


def event(component: str, title: str, details: str, style: str = "cyan") -> None:
    if Console is None:
        print(f"[{component}] {title}: {details}")
    else:
        Console().print(f"[bold {style}][{component}] {title}[/bold {style}] {details}")


def panel(title: str, lines: list[str], style: str = "cyan") -> None:
    if Console is None:
        print(f"\n=== {title} ===\n" + "\n".join(lines))
    else:
        Console().print(Panel("\n".join(lines), title=title, border_style=style))


def show_topics(topics: dict[str, list[dict[str, Any]]]) -> None:
    if Console is None:
        print(topics)
        return
    table = Table(title="Topicuri si abonati")
    table.add_column("Topic", style="cyan")
    table.add_column("Subscriber")
    table.add_column("Adresa")
    table.add_column("Tipuri")
    for topic, subscribers in topics.items():
        for subscriber in subscribers:
            accepted = ", ".join(subscriber["accepted_types"]) or "toate"
            table.add_row(
                topic,
                subscriber["subscriber_id"],
                f"{subscriber['host']}:{subscriber['port']}",
                accepted,
            )
    if not topics:
        table.add_row("-", "niciun abonat", "-", "-")
    Console().print(table)
