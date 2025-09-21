import os
from pathlib import Path
import re
import openai
from openai import AsyncOpenAI # Import the async client
import asyncio
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
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            full_text = f.read()

        bibliography_headers = [
            "references", "bibliography", "works cited", "literature cited"
        ]

        bibliography_regex = re.compile(
            r"^(?:" + "|".join(re.escape(h) for h in bibliography_headers) + r")\b",
            re.IGNORECASE | re.MULTILINE
        )

        match = bibliography_regex.search(full_text)
        if match:
            return full_text[:match.start()].strip()

        return full_text.strip()

    except Exception as e:
        print(f"Error processing text for bibliography filter: {e}")
        return ""


async def filter_paper(paper_path: Path, keywords, organism_keywords) -> tuple[bool, str]:
    """
    Analyzes a single scientific paper (text file) to determine if it discusses
    the growth rates of microorganisms.
    """
    file_name = os.path.basename(paper_path)
    print(f"Analyzing {file_name}...")

    main_text = _get_main_text(paper_path)

    if not main_text:
        return (False, "Not Relevant")

    text_lower = main_text.lower()

    found_relevant_info = any(keyword in text_lower for keyword in keywords)

    if found_relevant_info:
        return await analyze_paper_for_growth_rates_advanced(paper_path)
    else:
        return (False, "Not Relevant - Simple Keyword Filter")


async def analyze_paper_for_growth_rates_advanced(file_path: Path) -> tuple[bool, str]:
    """
    Analyzes a single scientific paper using the OpenAI API to confirm the topic.
    """
    try:
        with open(".env") as f:
            OPENAI_API_KEY = f.readlines()[0].split("=")[1].strip()
        openai.api_key = os.getenv("OPENAI_API_KEY", OPENAI_API_KEY)
    except Exception as e:
        print("Error: The .env file with your API key could not be found.")
        return (False, "API Key Not Set")

    if not openai.api_key or openai.api_key == "YOUR_API_KEY_HERE":
        return (False, "API Key Not Set")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            full_text = f.read()
    except Exception as e:
        return (False, "File Read Error")

    # Use the asynchronous client
    client = AsyncOpenAI(api_key=openai.api_key)

    prompt_text = (
        f"Analyze the following text from a scientific paper:\n\n---\n{full_text}\n\n"
        f"Does this text contain information regarding the growth rates of microorganisms? "
        f"Respond with a single word, either 'True' or 'False'. Do not add any other text, punctuation, or explanation."
    )

    try:
        # Note the use of the client to call the async method
        response = await client.chat.completions.create(
            model="gpt-5-nano",
            messages=[{"role": "user", "content": prompt_text}],
        )
        response_text = response.choices[0].message.content.strip().lower()

        if response_text == 'true':
            print(f"  - Confirmed: OpenAI API determined {file_path.name} is relevant.")
            return (True, "Relevant")
        elif response_text == 'false':
            print(f"  - Confirmed: OpenAI API determined {file_path.name} is not relevant.")
            return (False, "Not Relevant - API")
        else:
            return (False, f"Unexpected API response for {file_path.name}")

    except openai.AuthenticationError:
        return (False, "Authentication Failed")
    except Exception as e:
        return (False, f"API Error: {e}")


async def main():
    """
    Main asynchronous function to orchestrate the paper analysis.
    """
    keywords_list = [
        "growth rate", "doubling time", "specific growth rate", "proliferation",
        "cell division", "generation time", "biomass yield", "cell density",
        "optical density", "turbidity", "OD600", "fermentation", "culture",
        "cultivation", "bioreactor", "batch culture", "exponential phase",
        "log phase", "stationary phase", "lag phase", "growth kinetics", "growth parameters",
        "growth curve"
    ]

    organism_keywords_list = [
        "microorganism", "bacterium", "bacteria", "yeast", "e. coli", "fungus",
        "algae", "microbe", "saccharomyces", "bacillus", "prokaryote", "eukaryote",
        "fungi", "algae", "protozoa", "virus", "pathogen", "streptococcus", "staphylococcus",
        "pseudomonas", "bacillus subtilis", "aspergillus", "penicillium",
        "chlorella", "paramecium", "amoeba", "lactobacillus", "mycobacterium",
        "archaea", "cyanobacteria"
    ]

    papers_dir = Path("example-papers-txt-2")

    if not papers_dir.exists() or not papers_dir.is_dir():
        print(f"Directory {papers_dir} does not exist or is not a directory.")
        return

    # Create a list of all async tasks
    tasks = []
    for paper_path in papers_dir.iterdir():
        if paper_path.is_file() and paper_path.suffix.lower() == '.txt':
            tasks.append(filter_paper(paper_path, keywords_list, organism_keywords_list))

    # Run all tasks concurrently and get results
    if tasks:
        results = await asyncio.gather(*tasks)

        print("\n--- Analysis Summary ---")
        for i, paper_path in enumerate(papers_dir.iterdir()):
            if paper_path.is_file() and paper_path.suffix.lower() == '.txt':
                name = os.path.basename(paper_path)
                result, status_text = results[i]
                status = "✔️ Relevant" if result else f"❌ {status_text}"
                print(f"{name}: {status}")
        print("------------------------")


if __name__ == "__main__":
    asyncio.run(main())

    # End timer
    end_time = time.time()

    # Print elapsed time
    print(f"\nProgram took {end_time - start_time:.4f} seconds to run")