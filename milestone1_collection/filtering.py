from pathlib import Path
from openai import OpenAI
import pdfplumber
import os
import spacy
import re

try:
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    print('Success')
except OSError:
    print("SpaCy English model 'en_core_web_sm' not found.")
    print("Please run: python -m spacy download en_core_web_sm")
    # Exit the program if the model is not found, as it's a critical dependency.
    exit()


#with open(".env") as f:
#    OPENAI_API_KEY = f.readlines()[0].split("=")[1]
#client = OpenAI(api_key = OPENAI_API_KEY)


# response = client.responses.create(
#     model="gpt-5",
#     input="Write a short bedtime story about a unicorn."
# )
# print(response.output_text)


def filter_paper(file_path: Path, keywords, organism_keywords) -> bool:
    """
        Analyzes a single scientific paper (PDF) to determine if it discusses
        the growth rates of microorganisms.

        Args:
            file_path (str): The path to the PDF file.
            keywords (list): A list of keywords and phrases to search for.

        Returns:
            tuple: A tuple containing the file name and a boolean indicating
                   if the topic was found.
        """
    # A simple flag to track if any relevant keywords are found.
    found_relevant_info = False

    # Extract the file name from the full path.
    file_name = os.path.basename(file_path)

    # Use a try-except block to handle potential file-reading errors gracefully.
    try:
        # Open the PDF file using pdfplumber.
        with pdfplumber.open(file_path) as pdf:
            print(f"Analyzing {file_name}...")

            # Iterate through each page of the document.
            for page in pdf.pages:
                # Extract the text from the current page.
                text = page.extract_text()

                # Check if the text was successfully extracted.
                if text:
                    # Normalize the text to lowercase for case-insensitive searching.
                    text_lower = text.lower()

                    # Iterate through the list of keywords.
                    for keyword in keywords:
                        # Check if the keyword exists in the lowercased text.
                        if keyword in text_lower:
                            # A keyword has been found. We can set the flag to True
                            # and break the inner loops to save time.
                            found_relevant_info = True
                            print(f"  - Found keyword: '{keyword}'")
                            break  # Exit the keyword loop.

                # If a keyword was found on a previous page, no need to check further.
                if found_relevant_info:
                    break  # Exit the page loop.

    except Exception as e:
        # If an error occurs (e.g., file not found or corrupted), print a message.
        print(f"  - Error processing {file_name}: {e}")
        # The function will still return False for this file.

    '''
    if found_relevant_info:
        found_relevant_info_advanced = analyze_paper_for_growth_rates_advanced(file_path, keywords, organism_keywords)
    # Return the file name and the final result.
        return found_relevant_info_advanced
    else:
        return False
    '''
    return found_relevant_info

def analyze_paper_for_growth_rates_advanced(file_path: Path, growth_keywords, organism_keywords) -> bool:
    """
    Analyzes a single scientific paper (PDF) using advanced methods including
    contextual NLP and regular expressions to confirm the topic.

    Args:
        file_path (Path): The path to the PDF file.
        growth_keywords (list): Keywords related to growth rates.
        organism_keywords (list): Keywords related to microorganisms.

    Returns:
        bool: True if the topic is confirmed by advanced analysis, False otherwise.
    """
    found_contextual_info = False

    # Define a regex to find numerical values with specific units, like per hour.
    # This pattern looks for numbers followed by common units.
    rate_regex = re.compile(r"(\d+\.\d+|\d+)\s*(h|hr|hour|min|day|s)\^?(-?1)?", re.IGNORECASE)

    try:
        # Open the PDF file using pdfplumber.
        with pdfplumber.open(str(file_path)) as pdf:
            # Combine all text from the PDF into a single string for processing.
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

            # Process the full text with spaCy for contextual analysis.
            doc = nlp(full_text.lower())

            # Perform the contextual search for growth rates and organisms.
            for growth_keyword in growth_keywords:
                # Find all occurrences of the growth keyword.
                for match in re.finditer(r"\b" + re.escape(growth_keyword) + r"\b", doc.text):
                    start, end = match.span()

                    # Define a search window around the keyword (e.g., 200 characters).
                    # This is where we look for a microorganism name.
                    search_window = doc.text[max(0, start - 200):min(len(doc.text), end + 200)]

                    # Check for any organism keywords within the search window.
                    if any(organism_keyword in search_window for organism_keyword in organism_keywords):
                        print(f"  - Confirmed: Found '{growth_keyword}' in context with a microorganism.")
                        found_contextual_info = True
                        break # Exit the organism keyword loop
                if found_contextual_info:
                    break # Exit the growth keyword loop

            # Perform a secondary check using regular expressions for growth units.
            if not found_contextual_info:
                if rate_regex.search(doc.text):
                    print("  - Confirmed: Found numerical growth rate pattern.")
                    found_contextual_info = True

    except Exception as e:
        print(f"  - Error processing for advanced check: {e}")
        found_contextual_info = False

    return found_contextual_info


keywords_list = [
    "growth rate", "doubling time", "specific growth rate",
    "proliferation", "cell division", "generation time"
]

organism_keywords_list = [
    "microorganism", "bacterium", "bacteria", "yeast", "e. coli", "fungus",
    "algae", "microbe", "saccharomyces", "bacillus"
]

if __name__ == "__main__":
    papers_dir = Path("example-papers")
    analysis_results = {}
    if not papers_dir.exists() or not papers_dir.is_dir():
        print(f"Directory {papers_dir} does not exist or is not a directory.")
    else:
        for paper_path in papers_dir.iterdir():
            if paper_path.is_file():
                result = filter_paper(paper_path, keywords_list, organism_keywords_list)
                print(f"{paper_path.name}: {result}")
                file_name = os.path.basename(paper_path)
                analysis_results[file_name] = result

    print(analysis_results)
