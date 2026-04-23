"""
PDF Service
Converts email content to PDF documents for analysis
"""

import io
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Try to import reportlab, fallback to fpdf2 if not available
PDF_LIBRARY = None
try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT, TA_CENTER
    PDF_LIBRARY = 'reportlab'
    logger.info("Using reportlab for PDF generation")
except ImportError:
    try:
        from fpdf import FPDF
        PDF_LIBRARY = 'fpdf2'
        logger.info("Using fpdf2 for PDF generation")
    except ImportError:
        logger.warning("No PDF library available. Install reportlab or fpdf2.")


class PDFService:
    """Service for generating PDF documents from email content"""
    
    def __init__(self):
        self._available = PDF_LIBRARY is not None
    
    def is_available(self) -> bool:
        """Check if PDF generation is available"""
        return self._available
    
    def create_email_pdf(
        self,
        subject: str,
        sender: str,
        body: str,
        recipients: Optional[str] = None,
        received_at: Optional[datetime] = None
    ) -> Optional[bytes]:
        """
        Create a PDF document from email content
        
        Args:
            subject: Email subject
            sender: Sender email address
            body: Email body text
            recipients: Optional recipients
            received_at: Optional received timestamp
            
        Returns:
            PDF content as bytes, or None if generation fails
        """
        if not self._available:
            logger.error("PDF library not available")
            return None
        
        # Ensure recipients is always a string
        if isinstance(recipients, list):
            recipients = ', '.join(str(r) for r in recipients)
        elif recipients is not None:
            recipients = str(recipients)
        
        try:
            if PDF_LIBRARY == 'reportlab':
                return self._create_pdf_reportlab(subject, sender, body, recipients, received_at)
            elif PDF_LIBRARY == 'fpdf2':
                return self._create_pdf_fpdf(subject, sender, body, recipients, received_at)
            else:
                return None
        except Exception as e:
            logger.error(f"Error creating PDF: {e}")
            logger.error(f"Error creating PDF: {e}")
            return None
    
    def _create_pdf_reportlab(
        self,
        subject: str,
        sender: str,
        body: str,
        recipients: Optional[str] = None,
        received_at: Optional[datetime] = None
    ) -> bytes:
        """Create PDF using reportlab"""
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )
        
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'EmailTitle',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=12,
            textColor=colors.HexColor('#1a365d')
        )
        
        header_style = ParagraphStyle(
            'EmailHeader',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#4a5568'),
            spaceAfter=4
        )
        
        body_style = ParagraphStyle(
            'EmailBody',
            parent=styles['Normal'],
            fontSize=11,
            leading=16,
            spaceAfter=8,
            alignment=TA_LEFT
        )
        
        # Build content
        story = []
        
        # Title - "Email Document"
        story.append(Paragraph("Email Document", title_style))
        story.append(Spacer(1, 0.2*inch))
        
        # Email metadata table
        metadata = []
        metadata.append(['Subject:', subject or '(No Subject)'])
        metadata.append(['From:', sender or '(Unknown Sender)'])
        if recipients:
            metadata.append(['To:', recipients])
        if received_at:
            date_str = received_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(received_at, datetime) else str(received_at)
            metadata.append(['Date:', date_str])
        
        # Create metadata table
        table = Table(metadata, colWidths=[1.2*inch, 5.3*inch])
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#2d3748')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#4a5568')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(table)
        
        # Separator line - use a simple line instead of hr tag
        story.append(Spacer(1, 0.3*inch))
        from reportlab.platypus import HRFlowable
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#e2e8f0')))
        story.append(Spacer(1, 0.3*inch))
        
        # Email body
        if body:
            # Split body into paragraphs and add each
            paragraphs = body.split('\n')
            for para in paragraphs:
                para = para.strip()
                if para:
                    # Escape special characters for reportlab
                    para = para.replace('&', '&amp;')
                    para = para.replace('<', '&lt;')
                    para = para.replace('>', '&gt;')
                    story.append(Paragraph(para, body_style))
                else:
                    story.append(Spacer(1, 0.15*inch))
        else:
            story.append(Paragraph("<i>(No email body content)</i>", body_style))
        
        # Build PDF
        doc.build(story)
        
        buffer.seek(0)
        return buffer.getvalue()
    
    def _create_pdf_fpdf(
        self,
        subject: str,
        sender: str,
        body: str,
        recipients: Optional[str] = None,
        received_at: Optional[datetime] = None
    ) -> bytes:
        """Create PDF using fpdf2"""
        from fpdf import FPDF
        
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # Title
        pdf.set_font('Helvetica', 'B', 16)
        pdf.set_text_color(26, 54, 93)  # Dark blue
        pdf.cell(0, 10, 'Email Document', ln=True)
        pdf.ln(5)
        
        # Metadata
        pdf.set_font('Helvetica', '', 10)
        pdf.set_text_color(74, 85, 104)  # Gray
        
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(25, 7, 'Subject:', ln=False)
        pdf.set_font('Helvetica', '', 10)
        pdf.cell(0, 7, subject or '(No Subject)', ln=True)
        
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(25, 7, 'From:', ln=False)
        pdf.set_font('Helvetica', '', 10)
        pdf.cell(0, 7, sender or '(Unknown Sender)', ln=True)
        
        if recipients:
            pdf.set_font('Helvetica', 'B', 10)
            pdf.cell(25, 7, 'To:', ln=False)
            pdf.set_font('Helvetica', '', 10)
            pdf.cell(0, 7, recipients, ln=True)
        
        if received_at:
            date_str = received_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(received_at, datetime) else str(received_at)
            pdf.set_font('Helvetica', 'B', 10)
            pdf.cell(25, 7, 'Date:', ln=False)
            pdf.set_font('Helvetica', '', 10)
            pdf.cell(0, 7, date_str, ln=True)
        
        # Separator
        pdf.ln(5)
        pdf.set_draw_color(226, 232, 240)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(10)
        
        # Body
        pdf.set_font('Helvetica', '', 11)
        pdf.set_text_color(45, 55, 72)
        
        if body:
            # Handle multi-line text
            pdf.multi_cell(0, 7, body)
        else:
            pdf.set_font('Helvetica', 'I', 11)
            pdf.cell(0, 7, '(No email body content)', ln=True)
        
        return bytes(pdf.output())


# Singleton instance
_pdf_service: Optional[PDFService] = None


def get_pdf_service() -> PDFService:
    """Get the PDF service singleton"""
    global _pdf_service
    if _pdf_service is None:
        _pdf_service = PDFService()
    return _pdf_service
