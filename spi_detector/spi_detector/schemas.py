"""
Pydantic schemas for screenshot sensitivity analysis.
Forces strict JSON validation like the main spi_superhoover codebase.
"""

from pydantic import BaseModel, Field
from typing import List


class VisionAnalysisResponse(BaseModel):
    """
    Schema for vision-based screenshot sensitivity analysis.
    All fields are required for strict validation.
    """
    primary_intent: str = Field(
        ...,
        description="Primary purpose of the page (e.g., 'login', 'registration', 'account_page')"
    )
    sensitive: bool = Field(
        ...,
        description="Whether the screenshot contains sensitive personal information"
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence score from 0.0 to 1.0"
    )
    page_type: str = Field(
        ...,
        description="Type of page (e.g., 'login', 'form', 'dashboard', 'receipt')"
    )
    pii_types: List[str] = Field(
        default_factory=list,
        description="Types of PII detected (e.g., 'email', 'phone', 'address', 'name')"
    )
    quoted_evidence: List[str] = Field(
        default_factory=list,
        description="Exact quoted strings from the image that prove sensitivity"
    )
    reasons: List[str] = Field(
        default_factory=list,
        description="List of reasons for the sensitivity determination"
    )

    class Config:
        # Strict mode - no extra fields allowed
        extra = "forbid"

