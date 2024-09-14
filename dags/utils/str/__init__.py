def fmt_left(text):
    """Shifts all text in a string to the left until the first non-blank line touches the left border.

    Args:
      text: The input string.

    Returns:
      The shifted string.
    """

    lines = text.splitlines()
    shifted_lines = []

    # Find the index of the first non-blank line
    first_non_blank_line_index = next(i for i, line in enumerate(lines) if line.strip() != "")

    # Determine the indentation level of the first non-blank line
    first_non_blank_line_indent = len(lines[first_non_blank_line_index]) - len(
        lines[first_non_blank_line_index].lstrip())

    for line in lines:
        # Shift the line by the indentation level of the first non-blank line
        shifted_line = line[first_non_blank_line_indent:]
        shifted_lines.append(shifted_line)

    return "\n".join(shifted_lines)
