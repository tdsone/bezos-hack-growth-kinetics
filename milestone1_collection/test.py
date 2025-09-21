import os
from pathlib import Path
import spacy
import re

def _get_main_text(file_path: Path) -> str:
    """
    Reads a text file and returns the content up to the bibliography section.

    This function uses common bibliography section headers to identify and
    filter out the references.

    Args:
        file_path (Path): The path to the text file.

    Returns:
        str: The content of the file up to the bibliography, or the full
             content if no bibliography header is found.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            full_text = f.read()

        # Common headers for a bibliography or references section.
        bibliography_headers = [
            "references", "bibliography", "works cited", "literature cited"
        ]

        # Use a regex to find the start of the bibliography section.
        # It looks for a header on its own line, optionally followed by a number.
        bibliography_regex = re.compile(
            r"^(?:" + "|".join(re.escape(h) for h in bibliography_headers) + r")\b",
            re.IGNORECASE | re.MULTILINE
        )

        match = bibliography_regex.search(full_text)
        if match:
            # If a match is found, return the text from the beginning of the file
            # up to the start of the bibliography section.
            return full_text[:match.start()].strip()

        # If no bibliography header is found, return the entire text.
        return full_text.strip()

    except Exception as e:
        print(f"Error processing text for bibliography filter: {e}")
        return ""


if __name__ == "__main__":
    papers_dir = Path()
    text = _get_main_text("example-papers-txt/610010.txt")
    print(text)