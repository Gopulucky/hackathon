import PyPDF2

def extract_text_from_pdf(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            for page_num in range(len(reader.pages)):
                page = reader.pages[page_num]
                text += f"\n--- Page {page_num + 1} ---\n"
                text += page.extract_text()
            return text
    except Exception as e:
        return str(e)

if __name__ == "__main__":
    pdf_path = "UDAI (1).pdf"
    content = extract_text_from_pdf(pdf_path)
    with open("pdf_content.txt", "w", encoding="utf-8") as f:
        f.write(content)
    print("PDF content extracted to pdf_content.txt")
