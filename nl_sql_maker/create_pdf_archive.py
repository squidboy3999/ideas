import os
import argparse
from fpdf import FPDF, XPos, YPos

# A set of common directories and file extensions to ignore.
IGNORED_DIRS = {'.git', '__pycache__', '.vscode', '.idea', 'node_modules', 'venv'}
IGNORED_EXTENSIONS = {
    '.pyc', '.pyo', '.o', '.a', '.so', '.lib', '.dll', '.exe',
    '.img', '.iso', '.zip', '.tar', '.gz', '.rar', '.7z',
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.ico',
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
    '.mp3', '.wav', '.mp4', '.mov', '.avi', '.mkv', '.db'
}

def add_file_to_pdf(pdf, file_path, base_dir):
    """
    Reads the content of a file and adds it to the PDF object.
    The file's relative path is used as a header.
    """
    relative_path = os.path.relpath(file_path, base_dir)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        print(f"⚠️  Skipping non-UTF-8 file (likely binary): {relative_path}")
        return
    except Exception as e:
        print(f"❌ Error reading file {relative_path}: {e}")
        return

    # --- KEY CHANGE HERE ---
    # Sanitize content by encoding it to latin-1 and ignoring any characters
    # that can't be represented. This prevents errors with Unicode characters.
    content = content.encode('latin-1', 'ignore').decode('latin-1')
    # --- END OF CHANGE ---

    pdf.add_page()
    
    # --- Header (File Path) ---
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, relative_path, 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
    pdf.ln(5)

    # --- Body (File Content) ---
    pdf.set_font("Courier", "", 10)
    pdf.multi_cell(0, 5, content)
    
    print(f"✅ Added to PDF: {relative_path}")

def main():
    """
    Main function to parse arguments and generate the PDF.
    """
    parser = argparse.ArgumentParser(
        description="Recursively scan a directory and compile all text-based files into a single PDF.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("source_dir", help="The source directory to scan.")
    parser.add_argument("output_pdf", help="The name of the output PDF file (e.g., 'archive.pdf').")
    
    args = parser.parse_args()

    if not os.path.isdir(args.source_dir):
        print(f"Error: Source directory '{args.source_dir}' not found.")
        return

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)

    for root, dirs, files in os.walk(args.source_dir, topdown=True):
        dirs[:] = [d for d in dirs if d not in IGNORED_DIRS]
        files.sort()
        
        for filename in files:
            _, extension = os.path.splitext(filename)
            if extension.lower() in IGNORED_EXTENSIONS:
                print(f"⏭️  Skipping by extension: {filename}")
                continue

            file_path = os.path.join(root, filename)
            add_file_to_pdf(pdf, file_path, args.source_dir)
    
    try:
        pdf.output(args.output_pdf)
        print(f"\n🎉 Successfully created PDF: {args.output_pdf}")
    except Exception as e:
        print(f"\n❌ Failed to create PDF. Error: {e}")

if __name__ == "__main__":
    main()