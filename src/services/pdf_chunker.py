"""
PDF Chunker Service
Splits large PDFs into smaller chunks for parallel AI processing.
"""

import io
import os
import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)

# Azure CU works best with smaller chunks — 50 pages is reliable.
# 100+ pages per chunk can cause Azure CU to timeout.
DEFAULT_MAX_PAGES_PER_CHUNK = 50

# Smart page limit: for very large documents (e.g. 250-page offering circulars),
# only analyse the first N pages.  Cat bond key terms are usually on pages 5-10.
# Set to 0 to disable (analyse all pages).
DEFAULT_SMART_PAGE_LIMIT = 100


def get_max_pages_per_chunk() -> int:
    """Get the configurable max pages per chunk from env or default."""
    try:
        return int(os.getenv('PDF_CHUNK_MAX_PAGES', str(DEFAULT_MAX_PAGES_PER_CHUNK)))
    except (ValueError, TypeError):
        return DEFAULT_MAX_PAGES_PER_CHUNK


def get_smart_page_limit() -> int:
    """Get the smart page limit from env or default. 0 = no limit."""
    try:
        return int(os.getenv('PDF_SMART_PAGE_LIMIT', str(DEFAULT_SMART_PAGE_LIMIT)))
    except (ValueError, TypeError):
        return DEFAULT_SMART_PAGE_LIMIT


class PDFChunker:
    """
    Splits a PDF into smaller chunks of N pages each.
    Each chunk is returned as raw PDF bytes ready for upload / analysis.
    """

    def __init__(self, max_pages_per_chunk: int = None, smart_page_limit: int = None):
        self.max_pages = max_pages_per_chunk or get_max_pages_per_chunk()
        self.smart_page_limit = smart_page_limit if smart_page_limit is not None else get_smart_page_limit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def trim_to_limit(self, pdf_bytes: bytes) -> tuple:
        """
        If smart_page_limit > 0 and the PDF exceeds it, return a trimmed
        copy containing only the first N pages.
        
        Returns (trimmed_bytes, original_page_count, was_trimmed).
        If no trimming is needed, returns (original_bytes, page_count, False).
        """
        if not self.smart_page_limit or self.smart_page_limit <= 0:
            page_count = self.get_page_count(pdf_bytes)
            return pdf_bytes, page_count, False
        
        try:
            from pypdf import PdfReader, PdfWriter
            reader = PdfReader(io.BytesIO(pdf_bytes))
            total_pages = len(reader.pages)
            
            if total_pages <= self.smart_page_limit:
                return pdf_bytes, total_pages, False
            
            # Trim to first N pages
            writer = PdfWriter()
            for i in range(self.smart_page_limit):
                writer.add_page(reader.pages[i])
            
            buf = io.BytesIO()
            writer.write(buf)
            trimmed_bytes = buf.getvalue()
            
            logger.info(
                f"Smart page limit: trimmed PDF from {total_pages} to "
                f"{self.smart_page_limit} pages ({len(trimmed_bytes)} bytes)"
            )
            return trimmed_bytes, total_pages, True
        except Exception as e:
            logger.warning(f"Failed to trim PDF: {e}")
            page_count = self.get_page_count(pdf_bytes)
            return pdf_bytes, page_count, False

    def needs_chunking(self, pdf_bytes: bytes) -> bool:
        """Return True if the PDF has more pages than the configured limit."""
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            total_pages = len(reader.pages)
            logger.info(f"PDF has {total_pages} pages, chunk limit is {self.max_pages}")
            return total_pages > self.max_pages
        except Exception as e:
            logger.warning(f"Could not read PDF to check page count: {e}")
            return False

    def get_page_count(self, pdf_bytes: bytes) -> int:
        """Return the total number of pages in the PDF."""
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            return len(reader.pages)
        except Exception as e:
            logger.warning(f"Could not read PDF page count: {e}")
            return 0

    def split(self, pdf_bytes: bytes) -> List[Tuple[bytes, int, int]]:
        """
        Split a PDF into chunks of at most ``self.max_pages`` pages.

        Returns a list of tuples:
            (chunk_pdf_bytes, start_page_1based, end_page_1based)

        If the document is within the limit, a single-element list with
        the original bytes is returned (no re-encoding overhead).
        """
        from pypdf import PdfReader, PdfWriter

        reader = PdfReader(io.BytesIO(pdf_bytes))
        total_pages = len(reader.pages)

        if total_pages <= self.max_pages:
            logger.info(f"PDF has {total_pages} pages — no chunking needed")
            return [(pdf_bytes, 1, total_pages)]

        chunks: List[Tuple[bytes, int, int]] = []
        for start in range(0, total_pages, self.max_pages):
            end = min(start + self.max_pages, total_pages)
            writer = PdfWriter()
            for page_idx in range(start, end):
                writer.add_page(reader.pages[page_idx])

            buf = io.BytesIO()
            writer.write(buf)
            chunk_bytes = buf.getvalue()

            start_page = start + 1        # 1-based
            end_page = end                # inclusive, 1-based
            chunks.append((chunk_bytes, start_page, end_page))

            logger.info(
                f"Created chunk: pages {start_page}–{end_page} "
                f"({end - start} pages, {len(chunk_bytes)} bytes)"
            )

        logger.info(
            f"Split PDF ({total_pages} pages) into {len(chunks)} chunks "
            f"of up to {self.max_pages} pages each"
        )
        return chunks


# ---- Singleton --------------------------------------------------------

_chunker = None


def get_pdf_chunker() -> PDFChunker:
    """Get the PDF chunker singleton."""
    global _chunker
    if _chunker is None:
        _chunker = PDFChunker()
    return _chunker


def reset_chunker():
    """Reset the singleton (useful after config changes)."""
    global _chunker
    _chunker = None
