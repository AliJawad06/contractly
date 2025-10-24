from bs4 import BeautifulSoup
from pydantic import BaseModel
from typing import List
from google import genai
import json
from schema import schema
# Allowed education values:
# HS Diploma, Associate's, Bachelor's, Masters, PHD


def getSkills(description, custom_id):
    """
    Extract structured skill info, years of experience, education level, and certifications
    from a job description using Gemini structured output parsing.
    """

    class Skills(BaseModel):
        hardSkills: List[str]
        softSkills: List[str]

    class CalendarEvent(BaseModel):
        yoE: int
        required: Skills
        preferred: Skills
        minEducation: str
        certsLicences: List[str]
        categorys: List[str]

    # Instruction string
    instructions = (
        "Extract structured information from the job description. "
        "For hard and soft skills, keep each skill to a maximum of two words or an acronym. "
        "If years of experience is given as a range, choose the lowest number. "
        "List certifications or licenses (certsLicences), using a maximum of two words or two acronyms. "
        "The field 'minEducation' can only be one of: HS Diploma, Associate's, Bachelor's, Masters, PHD. "
        "The field 'categorys' can be a list of 1 to max 3 category:subcategory pair. A category is one of the 5 and subcategory is before the semicolon."
    )

    categoryOptions = '''1. Healthcare & Medical

Nursing: RN (various specializations), LPN/LVN, CNA, Nurse Practitioners, Nurse Managers
Allied Health: Physical Therapists, Occupational Therapists, Respiratory Therapists, Speech Language Pathologists
Medical Imaging & Diagnostics: Radiology Techs, CT Technologists, MRI Technologists, Ultrasound Techs, Nuclear Medicine
Laboratory & Clinical: Medical Lab Scientists, Phlebotomists, Lab Technicians
Surgical & Procedural: Surgical Techs, Cath Lab Techs, OR Staff
Pharmacy: Pharmacists, Pharmacy Technicians
Physicians & Advanced Practitioners: Physicians (various specialties), Physician Assistants
Healthcare Administration: Directors of Nursing, Clinical Managers, Medical Staff Coordinators

2. Information Technology & Software Development

Software Engineering: Java Developers, Python Developers, Full Stack Developers, .NET Developers
Cloud & Infrastructure: AWS Engineers, Azure Engineers, GCP Engineers, DevOps Engineers, SRE
Data & Analytics: Data Engineers, Data Scientists, Data Analysts, Business Intelligence Developers
Enterprise Systems: SAP Consultants, Oracle Developers, Salesforce Developers, ServiceNow Developers
Database: DBAs (Oracle, SQL Server, MongoDB, etc.)
Quality Assurance: QA Engineers, SDET, Test Automation Engineers
Security: Cybersecurity Engineers, Security Analysts, IAM Engineers
Project Management: IT Project Managers, Scrum Masters, Product Owners

3. Engineering & Manufacturing

Mechanical Engineering: Design Engineers, Process Engineers, Manufacturing Engineers
Electrical Engineering: Electrical Engineers, Controls Engineers, Power Systems Engineers
Production & Operations: Machine Operators, Production Associates, Assembly Technicians
Quality: Quality Engineers, Quality Inspectors, Quality Assurance Specialists
Maintenance: Maintenance Technicians, Electricians, HVAC Technicians
Supply Chain: Material Handlers, Warehouse Associates, Buyers, Planners
Product Development: R&D Engineers, Design Engineers, Prototype Engineers

4. Business & Finance

Accounting & Finance: Accountants, Financial Analysts, Accounts Payable/Receivable Specialists
Human Resources: HR Generalists, Recruiters, HR Business Partners, Payroll Specialists
Project Management: Project Managers, Program Managers, Project Coordinators
Business Analysis: Business Analysts, Systems Analysts, Product Managers
Marketing & Communications: Marketing Managers, Content Creators, Communications Specialists
Sales: Sales Representatives, Account Managers, Business Development
Administration: Executive Assistants, Administrative Assistants, Office Managers

5. Skilled Trades & Construction

Construction Management: Project Managers, Superintendents, Estimators
Electrical: Electricians, Electrical Designers
Mechanical Trades: HVAC Technicians, Plumbers, Pipefitters
Fabrication: Welders, Machinists, CNC Operators
Carpentry & Finishing: Carpenters, Painters, Finishers
Heavy Equipment: Equipment Operators, Diesel Mechanics
Infrastructure: Network Technicians (telecom), Fiber Technicians, Cable Installers'''

    # Clean the job description
    if not description:
        return None


    # Create the prompt
    full_prompt = f"Job Description:\n{description}"

    # Generate response

    # Return structured result with custom_id
    obj = {
        "key": str(custom_id),
        "request": {
        "contents": [{
            "parts": [{'text': full_prompt}],
            "role": 'user'
        }], 
        "generationConfig": {
            "responseMimeType": "application/json",
            "_responseJsonSchema": schema        
            },
    "systemInstruction": {
        "parts": [{"text": f"{instructions}\n\nCategory Options:\n{categoryOptions}"}],
        "role": "user"
        }
      },  
    } 
    obj_str = json.dumps(obj, indent=2)
    return obj_str