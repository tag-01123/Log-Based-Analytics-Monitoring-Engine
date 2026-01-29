import re

# Parse a single log line and return structured data (dict)
def parse_log_line(line: str):
    """
    Expected log format:
    <timestamp> <level> <service> <message>
    """

    log_pattern = (
        r'^(?P<timestamp>\S+)\s+'
        r'(?P<level>\S+)\s+'
        r'(?P<service>\S+)\s+'
        r'(?P<message>.+)$'
    )

    match = re.match(log_pattern, line)

    if match:
        # groupdict() converts named groups into a dictionary
        return match.groupdict()

    return None
