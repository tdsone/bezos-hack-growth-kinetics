import pdfplumber
from pathlib import Path


# --- CONFIGURATION ---
# IMPORTANT: Change this variable to the name of the PDF file you want to test.
pdf_file_path = "your_document.pdf"



def extract_text_from_pdf(file_path):
    """
    Opens a PDF file and extracts text from each page.

    Args:
        file_path (str): The path to the PDF file.
    """
    try:
        # Use a 'with' statement to ensure the PDF file is closed properly
        with pdfplumber.open(file_path) as pdf:
            print(f"Successfully opened '{file_path}'.")

            # Loop through each page in the PDF
            for i, page in enumerate(pdf.pages):
                print(f"\n--- Extracting text from Page {i + 1} ---")

                # Extract the text from the current page
                text = page.extract_text()

                # Check if text was found on the page
                if text:
                    print(text)
                else:
                    print("No text found on this page.")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        print("Please make sure the file exists and the name is correct.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    extract_text_from_pdf("example-papers/044735v1.full.pdf")
