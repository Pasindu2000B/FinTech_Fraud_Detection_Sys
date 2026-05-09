from fpdf import FPDF
from datetime import datetime

def generate_pdf_report(stats, merchant_fraud_data, output_path):
    try:
        pdf = FPDF()
        pdf.add_page()
        
        pdf.set_font("Arial", 'B', 16)
        pdf.cell(200, 15, txt="FINTECH RECONCILIATION REPORT", ln=True, align='C')
        pdf.set_font("Arial", size=10)
        pdf.cell(200, 10, txt=f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align='C')
        pdf.ln(10)
        
        pdf.set_font("Arial", 'B', 12)
        pdf.set_fill_color(200, 220, 255)
        pdf.cell(100, 10, "Description", 1, 0, 'C', True)
        pdf.cell(90, 10, "Value", 1, 1, 'C', True)
        
        pdf.set_font("Arial", size=12)
        pdf.cell(100, 10, "Total Transactions Processed", 1)
        pdf.cell(90, 10, f"{stats['total_count']}", 1, 1, 'R')
        
        pdf.cell(100, 10, "Total Ingress Amount ($)", 1)
        pdf.cell(90, 10, f"{stats['total_amount']:,.2f}", 1, 1, 'R')
        
        pdf.cell(100, 10, "Validated Safe Amount ($)", 1)
        pdf.cell(90, 10, f"{stats['valid_amount']:,.2f}", 1, 1, 'R')
        
        pdf.set_text_color(255, 0, 0)
        pdf.cell(100, 10, "Blocked Fraud Amount ($)", 1)
        pdf.cell(90, 10, f"{stats['fraud_amount']:,.2f}", 1, 1, 'R')
        
        pdf.ln(15)
        
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Arial", 'B', 14)
        pdf.cell(200, 10, "Fraud Attempts by Merchant Category", ln=True, align='L')
        
        pdf.set_font("Arial", 'B', 11)
        pdf.set_fill_color(255, 200, 200)
        pdf.cell(80, 10, "Merchant Category", 1, 0, 'C', True)
        pdf.cell(40, 10, "Fraud Count", 1, 0, 'C', True)
        pdf.cell(70, 10, "Total Amount ($)", 1, 1, 'C', True)
        
        pdf.set_font("Arial", size=11)
        for item in merchant_fraud_data:
            cat_name = str(item['category']) if item['category'] else "Unknown"
            pdf.cell(80, 10, cat_name, 1)
            pdf.cell(40, 10, str(item['count']), 1, 0, 'C')
            pdf.cell(70, 10, f"{item['amount']:,.2f}", 1, 1, 'R')
        
        pdf.output(output_path)
        print("PDF Report generated")
        
    except Exception:
        print("Failed to generate PDF")