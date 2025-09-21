import os
import PyPDF2

def convert_pdfs_to_text(input_folder, output_folder):
    """
    Converts all PDF files in a given folder to text files.

    Args:
        input_folder (str): The path to the folder containing PDF files.
        output_folder (str): The path where the converted text files will be saved.
    """
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created output folder: {output_folder}")

    # Get a list of all files in the input folder
    files = os.listdir(input_folder)
    
    # Filter for PDF files
    pdf_files = [f for f in files if f.endswith('.pdf')]
    
    if not pdf_files:
        print(f"No PDF files found in '{input_folder}'.")
        return

    print(f"Found {len(pdf_files)} PDF files. Starting conversion...")

    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_folder, pdf_file)
        text_file_name = os.path.splitext(pdf_file)[0] + '.txt'
        text_path = os.path.join(output_folder, text_file_name)

        # Check if the output text file already exists
        if os.path.exists(text_path):
            print(f"Skipping '{pdf_file}': Corresponding text file already exists.")
            continue

        try:
            with open(pdf_path, 'rb') as pdf_file_obj:
                pdf_reader = PyPDF2.PdfReader(pdf_file_obj)
                
                text_content = ""
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text_content += page.extract_text() or ""
                    print(page_num)
                
                with open(text_path, 'w', encoding='utf-8') as text_file:
                    text_file.write(text_content)
                
                print(f"Successfully converted '{pdf_file}' to '{text_file_name}'.")

        except Exception as e:
            print(f"Error converting '{pdf_file}': {e}")
            
    print("Conversion process completed.")

# --- Main execution block ---
if __name__ == "__main__":
    # Define your input and output folder paths here
    # Example:
    # input_folder = "C:/Users/YourUser/Documents/MyPDFs"
    # output_folder = "C:/Users/YourUser/Documents/ConvertedTexts"

    # Replace 'input_pdfs' with the path to your folder containing PDF files.
    input_folder_path = "biorxiv-papers-2"
    
    # Replace 'output_texts' with your desired output folder name.
    output_folder_path = "example-papers-txt-2"

    convert_pdfs_to_text(input_folder_path, output_folder_path)
