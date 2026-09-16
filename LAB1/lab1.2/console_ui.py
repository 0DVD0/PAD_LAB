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


def topics_table(response) -> None:
    if Console is None:
        for topic in response.topics:
            print(topic.topic, [item.subscriber_id for item in topic.subscribers])
        return
    table = Table(title="Topicuri gRPC")
    table.add_column("Topic")
    table.add_column("Subscriber")
    table.add_column("Endpoint")
    table.add_column("Tipuri")
    for topic in response.topics:
        for item in topic.subscribers:
            table.add_row(topic.topic, item.subscriber_id, f"{item.host}:{item.port}", ", ".join(item.accepted_types) or "toate")
    if not response.topics:
        table.add_row("-", "niciun abonat", "-", "-")
    Console().print(table)
