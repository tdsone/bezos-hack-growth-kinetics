import os
from pathlib import Path
import spacy
import re
import time

# Start timer
start_time = time.time()

# Load the spaCy English model. This needs to be installed first.
# Run 'python -m spacy download en_core_web_sm' in your terminal.
# The 'disable' argument improves performance by skipping unnecessary components.
try:
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
except OSError:
    print("SpaCy English model 'en_core_web_sm' not found.")
    print("Please run: python -m spacy download en_core_web_sm")
    # Exit the program if the model is not found, as it's a critical dependency.
    exit()


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

def filter_paper(file_path: Path, keywords, organism_keywords) -> (bool, str):
    """
    Analyzes a single scientific paper (text file) to determine if it discusses
    the growth rates of microorganisms. This function performs an initial
    keyword check before calling the more advanced analysis.

    Args:
        file_path (Path): The path to the text file.
        keywords (list): A list of keywords and phrases to search for.
        organism_keywords (list): Keywords related to microorganisms.

    Returns:
        tuple: A tuple containing a boolean indicating if the topic was found,
               and a string with the confirmation message.
    """
    # A simple flag to track if any relevant keywords are found.
    found_relevant_info = False

    # Extract the file name from the full path.
    file_name = os.path.basename(file_path)
    print(f"Analyzing {file_name}...")

    # Get the main body text, excluding the bibliography.
    main_text = _get_main_text(file_path)

    # Check if the text was successfully read and filtered.
    if not main_text:
        return (False, "Not Relevant")

    # Normalize the text to lowercase for case-insensitive searching.
    text_lower = main_text.lower()

    for keyword in keywords:
        # Check if the keyword exists in the lowercased text.
        if keyword in text_lower:
            found_relevant_info = True
            print(f"  - Found simple keyword: '{keyword}'")
            break  # Exit the keyword loop.


    if found_relevant_info:
        # If the basic keyword check passes, perform the advanced analysis.
        return analyze_paper_for_growth_rates_advanced(text_lower, keywords, organism_keywords)
    else:
        # If no basic keywords are found, there's no need for advanced analysis.
        return (False, "Not Relevant")


def analyze_paper_for_growth_rates_advanced(text, growth_keywords, organism_keywords) -> (bool, str):
    """
    Analyzes a single scientific paper (text file) using advanced methods including
    contextual NLP and regular expressions to confirm the topic.

    Args:
        file_path (Path): The path to the text file.
        growth_keywords (list): Keywords related to growth rates.
        organism_keywords (list): Keywords related to microorganisms.

    Returns:
        tuple: A tuple containing a boolean indicating if the topic is confirmed
               by advanced analysis, and a string with the confirmation message.
    """
    found_contextual_info = False
    confirmation_message = ""

    # Define a regex to find numerical values with specific units.
    rate_regex = re.compile(r"(\d+\.\d+|\d+)\s*(h|hr|hour|min|day|s)\^?(-?1)?", re.IGNORECASE)

    try:
        # Open the text file and read the full content.

        # Process the full text with spaCy for contextual analysis.
        doc = nlp(text.lower())

        if not found_contextual_info:
            if rate_regex.search(doc.text):
                confirmation_message = "Found numerical growth rate pattern."
                print(f"  - Confirmed: {confirmation_message}")
                for growth_keyword in growth_keywords:
                    for match in re.finditer(r"\b" + re.escape(growth_keyword) + r"\b", doc.text):
                        start, end = match.span()

                        # Define a search window around the keyword.
                        search_window = doc.text[max(0, start - 500):min(len(doc.text), end + 500)]

                        found_organism = None
                        # Check for any organism keywords within the search window.
                        for organism_keyword in organism_keywords:
                            if organism_keyword in search_window:
                                found_organism = organism_keyword
                                break

                        if found_organism:
                            confirmation_message = f"Found '{growth_keyword}' in context with '{found_organism}'."
                            print(f"  - Confirmed: {confirmation_message}")
                            found_contextual_info = True
                            break  # Exit the organism keyword loop
                    if found_contextual_info:
                        break  # Exit the growth keyword loop

    except Exception as e:
        print(f"  - Error processing for advanced check: {e}")
        return (False, "Error during advanced analysis.")

    return (found_contextual_info, confirmation_message if found_contextual_info else "Not Relevant")


keywords_list = [
    # Original keywords
    "growth rate", "doubling time", "specific growth rate", "cell division", "generation time",

    # Proposed expansions (quantitative metrics)
    "biomass yield", "cell density", "optical density", "turbidity", "OD600",

    # Proposed expansions (process & conditions)
    "fermentation", "batch culture",

    # Proposed expansions (time-based)
    "exponential phase", "log phase", "stationary phase", "lag phase",

    # Proposed expansions (general terms)
    "growth kinetics", "growth parameters", "growth curve"
]

organism_keywords_list = [
    # Original keywords
    "microorganism", "bacterium", "bacteria", "yeast", "e. coli", "fungus",
    "algae", "microbe", "saccharomyces", "bacillus",

    # Proposed expansions (general)
    "prokaryote", "eukaryote", "fungi", "algae", "protozoa",

    # Proposed expansions (specific genus/species)
    "streptococcus", "staphylococcus", "pseudomonas", "bacillus subtilis",
    "aspergillus", "penicillium", "chlorella", "paramecium", "amoeba",
    "lactobacillus", "mycobacterium",

    # Proposed expansions (domain-level)
    "archaea", "cyanobacteria"
]

if __name__ == "__main__":
    papers_dir = Path("example-papers-txt-2")
    analysis_results = {}
    if not papers_dir.exists() or not papers_dir.is_dir():
        print(f"Directory {papers_dir} does not exist or is not a directory.")
        print("Please create a folder named 'example-papers-txt' and place your TXT files inside.")
    else:
        for paper_path in papers_dir.iterdir():
            if paper_path.is_file() and paper_path.suffix.lower() == '.txt':
                # The function now returns a tuple (bool, str)
                result = filter_paper(paper_path, keywords_list, organism_keywords_list)
                print(f"Final Result for {paper_path.name}: {result[0]}\n")
                file_name = os.path.basename(paper_path)
                analysis_results[file_name] = result

    if analysis_results:
        print("--- Analysis Summary ---")
        for name, result_tuple in analysis_results.items():
            found, message = result_tuple
            status = "✔️ Relevant" if found else "❌ Not Relevant"
            if found:
                print(f"{name}: {status} ({message})")
            else:
                print(f"{name}: {status}")
        print("------------------------")

# End timer
end_time = time.time()

# Print elapsed time
print(f"Program took {end_time - start_time:.4f} seconds to run")