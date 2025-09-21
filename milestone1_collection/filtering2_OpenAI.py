import os
from pathlib import Path
import re
import openai

import time

# Start timer
start_time = time.time()

# --- Instructions for using the OpenAI API ---
# 1. Install the OpenAI Python library: pip install openai
# 2. Get your API key from the OpenAI website.
# 3. Set your API key as an environment variable (recommended) or replace 'YOUR_API_KEY_HERE'.
#    Example: os.environ['OPENAI_API_KEY'] = 'sk-...'

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


def filter_paper(file_path: Path, keywords, organism_keywords) -> bool:
    """
    Analyzes a single scientific paper (text file) to determine if it discusses
    the growth rates of microorganisms. This function performs an initial
    keyword check before calling the more advanced analysis.

    Args:
        file_path (Path): The path to the text file.
        keywords (list): A list of keywords and phrases to search for.
        organism_keywords (list): Keywords related to microorganisms.

    Returns:
        bool: True if the topic was found, False otherwise.
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


def analyze_paper_for_growth_rates_advanced(file_path: Path, growth_keywords, organism_keywords) -> bool:
    """
    Analyzes a single scientific paper using the OpenAI API to confirm the topic.

    Args:
        file_path (Path): The path to the text file.
        growth_keywords (list): Keywords related to growth rates (not used in this version).
        organism_keywords (list): Keywords related to microorganisms (not used in this version).

    Returns:
        bool: True if the topic is confirmed by advanced analysis, False otherwise.
    """
    # Set the OpenAI API key. Replace 'YOUR_API_KEY_HERE' or use an environment variable.
    with open(".env") as f:
        OPENAI_API_KEY = f.readlines()[0].split("=")[1]
    openai.api_key = os.getenv("OPENAI_API_KEY", OPENAI_API_KEY)

    # Read the full text of the paper.
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            full_text = f.read()
    except Exception as e:
        print(f"  - Error reading file for advanced analysis: {e}")
        return False

    if not openai.api_key or openai.api_key == "YOUR_API_KEY_HERE":
        print("OpenAI API key is not set. Please set it to run the advanced analysis.")
        return False

    # Construct a concise prompt for the language model.
    # The prompt instructs the model to act as a scientific analyst and respond with a single word.
    prompt_text = (
        f"Analyze the following text from a scientific paper:\n\n---\n{full_text}\n---\n\n"
        f"Does this text contain information regarding the growth rates of microorganisms? "
        f"Respond with a single word, either 'True' or 'False'. Do not add any other text, punctuation, or explanation."
    )

    try:
        # Note: GPT-5 is not yet available, so we are using the 'gpt-4o' model as a placeholder.
        # You can replace this with 'gpt-5' once it's released.
        response = openai.chat.completions.create(
            model="gpt-5-nano",
            messages=[{"role": "user", "content": prompt_text}],
        )
        # Extract the model's response and clean it up.
        response_text = response.choices[0].message.content.strip().lower()

        # Parse the single-word response into a boolean.
        if response_text == 'true':
            print("  - Confirmed: OpenAI API determined the paper is relevant.")
            return True
        elif response_text == 'false':
            print("  - Confirmed: OpenAI API determined the paper is not relevant.")
            return False
        else:
            print(f"  - Unexpected response from API: '{response_text}'")
            return False

    except openai.AuthenticationError:
        print("  - Authentication failed. Please check your OpenAI API key.")
        return False
    except Exception as e:
        print(f"  - Error calling OpenAI API: {e}")
        return False


keywords_list = [
    # Original keywords
    "growth rate", "doubling time", "specific growth rate",
    "proliferation", "cell division", "generation time",

    # Proposed expansions (quantitative metrics)
    "biomass yield", "cell density", "optical density", "turbidity", "OD600",

    # Proposed expansions (process & conditions)
    "fermentation", "culture", "cultivation", "bioreactor", "batch culture",

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
    "prokaryote", "eukaryote", "fungi", "algae", "protozoa", "virus", "pathogen",

    # Proposed expansions (specific genus/species)
    "streptococcus", "staphylococcus", "pseudomonas", "bacillus subtilis",
    "aspergillus", "penicillium", "chlorella", "paramecium", "amoeba",
    "lactobacillus", "mycobacterium",

    # Proposed expansions (domain-level)
    "archaea", "cyanobacteria"
]

if __name__ == "__main__":
    papers_dir = Path("example-papers-txt")
    analysis_results = {}
    if not papers_dir.exists() or not papers_dir.is_dir():
        print(f"Directory {papers_dir} does not exist or is not a directory.")
        print("Please create a folder named 'example-papers' and place your TXT files inside.")
    else:
        for paper_path in papers_dir.iterdir():
            if paper_path.is_file() and paper_path.suffix.lower() == '.txt':
                result = filter_paper(paper_path, keywords_list, organism_keywords_list)
                print(f"Final Result for {paper_path.name}: {result}\n")
                file_name = os.path.basename(paper_path)
                analysis_results[file_name] = result

    if analysis_results:
        print("--- Analysis Summary ---")
        for name, found in analysis_results.items():
            status = "✔️ Relevant" if found else "❌ Not Relevant"
            print(f"{name}: {status}")
        print("------------------------")

# End timer
end_time = time.time()

# Print elapsed time
print(f"Program took {end_time - start_time:.4f} seconds to run")
