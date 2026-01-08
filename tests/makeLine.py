from bs4 import BeautifulSoup
from openai import AsyncOpenAI
from pydantic import BaseModel
from typing import List
from openai.lib._pydantic import to_strict_json_schema, model_json_schema
from openai import AsyncOpenAI

 #Allowed education values:
 #HS Diploma, Associate's, Bachelor's, Masters, PHD
 

def getSkills(description,custom_id):
    """
    Extract structured skill info, years of experience, education level, and certifications
    from a job description using GPT structured output parsing.
    
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

    #🧠 Instruction string
    instructions = '''
        Extract structured information from the job description. 
        For hard and soft skills, keep each skill to a maximum of two words or an acronym. 
        If years of experience is given as a range, choose the lowest number. 
        List certifications or licenses (certsLicences), using a maximum of two words or two acronyms. 
        The field 'minEducation' can only be one of: HS Diploma, Associate's, Bachelor's, Masters, PHD.
        the field 'categorys' can be a list of 1 to max 3 category:subcategory pair. A category is one of the 5 and subcategory is before the semicolon.

        This is an example description. Field IT Support Engineer Location - Kuwait: KW HO Kuwait HO NKGTC WLL, KW DC Suleibiya PWC Warehouse Onsite Dispatch Support SkySys is seeing resources to help assist with Field Servies Support in the following areas: - Desktop/End User Support - Smarts Hands & Feet / Data Center Support Job Requirements: Technical Minimum 2 to 3 years of strong experience providing IT Infrastructure field support which includes troubleshooting hardware, software, and operating system issues and problems and fixing the issues without impacting / violating the hardware warranty or customer security compliance requirements. •Experience in installing, technical troubleshooting and fixing desktops, printers, laptop, and other computer peripherals hardware problems as well as desktop applications. Basic knowledge of enterprise LAN and WAN setups and concepts. Ability to perform smart hand activity under instruction-based activities at sites. Ability to lift / move computer equipment weighing up to 50 lbs. Expert in desk-side support and PC break/fix including basic administration of Windows O/S and MAC/Linux OS (preferred). Usage Knowledge of TCP/IP networking, DNS, DHCP, VPN, and RDP. Smart hand support for peripheral and networking hardware, including, but not limited to monitors, keyboards, mice, printers, fax machines, scanners, routers, wireless routers, switches, firewalls, racks, cabinets, multi-port data termination panels all under 'Smart Hands' capability. Ability to troubleshoot issues with systems and networks using good deductive reasoning skills and troubleshooting & resolving issues related to end user network cabling. · Experienced in repeat call analysis and developing preventive actions · Experienced in Problem management Excellent written and oral communications skills with clients and management as well as people skills. Ability to work with deadlines and complete tasks on time. Takes proactive ownership and works with sense of urgency Preferably with an Associate Degree in Electronics and CompTIA A+ Certification. · May have other vendor certifications from OEMs (Dell, Toshiba, Lenovo, Client) Experience of ticketing tools (ServiceNow / Remedy etc.), _ Non-Technical_ Good Customer management skill, Good in oral and written communication Able to interact and work with customers at different levels. Driven and result oriented. Passionate about the work · Ability to work independently or as part of a team · Ability to complete tasks effectively with minimal supervision · Must be available to work flexible work schedules
        
        This is the corresponding structured information. 
          "calendar_event": {
    "yoE": 2,
    "required": {
      "hardSkills": [
        "IT support",
        "Hardware troubleshooting",
        "Software troubleshooting",
        "OS troubleshooting",
        "Desktop support",
        "Printer support",
        "Laptop repair",
        "Peripherals support",
        "Desktop applications",
        "LAN",
        "WAN",
        "Smart Hands",
        "Deskside support",
        "Break fix",
        "Windows admin",
        "Mac OS",
        "Linux OS",
        "TCP/IP",
        "DNS",
        "DHCP",
        "VPN",
        "RDP",
        "Networking hardware",
        "Routers",
        "Switches",
        "Firewalls",
        "Racks",
        "Cabinets",
        "Patch panels",
        "Network cabling",
        "Repeat analysis",
        "Preventive actions",
        "Problem management",
        "Ticketing tools",
        "ServiceNow",
        "Remedy"
      ],
      "softSkills": [
        "Communication",
        "Customer management",
        "People skills",
        "Deadline driven",
        "Time management",
        "Proactive ownership",
        "Urgency",
        "Result oriented",
        "Passionate",
        "Teamwork",
        "Independent",
        "Minimal supervision",
        "Flexible schedule",
        "Deductive reasoning",
        "Problem solving"
      ]
    },
    "preferred": {
      "hardSkills": [],
      "softSkills": []
    },
    "minEducation": "Associate's",
    "certsLicences": [
      "CompTIA A+"
    ],
    "categorys": [
      "IT:Field Support"
    ]
  }
    '''

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
Infrastructure: Network Technicians (telecom), Fiber Technicians, Cable Installers '''

    #🧹 Clean the job description
    if not description:
        return None


    response = {
                "custom_id": str(custom_id),
                "method": "POST",
                "url":"/v1/responses",
                "body": {
                "model":"gpt-4.1-mini",   #✅ faster and cheaper structured model
                "input":[
                    {"role": "system", "content": instructions},
                    {"role": "user", "content": description},
                ],
                "text": {
                            "format": {
                            "type": "json_schema",
                            "name": "response_schema",
                            "strict": True,
                            "schema": {
                                "type": "object",
                                "properties": {
                                "calendar_event": {
                                    "type": "object",
                                    "properties": {
                                    "yoE": {
                                        "type": "integer",
                                        "description": "Years of experience"
                                    },
                                    "required": {
                                        "type": "object",
                                        "properties": {
                                        "hardSkills": {
                                            "type": "array",
                                            "items": {
                                            "type": "string"
                                            },
                                            "description": "List of required hard skills"
                                        },
                                        "softSkills": {
                                            "type": "array",
                                            "items": {
                                            "type": "string"
                                            },
                                            "description": "List of required soft skills"
                                        }
                                        },
                                        "required": ["hardSkills", "softSkills"],
                                        "additionalProperties": False
                                    },
                                    "preferred": {
                                        "type": "object",
                                        "properties": {
                                        "hardSkills": {
                                            "type": "array",
                                            "items": {
                                            "type": "string"
                                            },
                                            "description": "List of preferred hard skills"
                                        },
                                        "softSkills": {
                                            "type": "array",
                                            "items": {
                                            "type": "string"
                                            },
                                            "description": "List of preferred soft skills"
                                        }
                                        },
                                        "required": ["hardSkills", "softSkills"],
                                        "additionalProperties": False
                                    },
                                    "minEducation": {
                                        "type": "string",
                                        "description": "Minimum education requirement"
                                    },
                                    "certsLicences": {
                                        "type": "array",
                                        "items": {
                                        "type": "string"
                                        },
                                        "description": "List of certifications and licenses"
                                    },
                                    "categorys": {
                                        "type": "array",
                                        "items": {
                                        "type": "string"
                                        },
                                        "description": "List of categories"
                                    }
                                    },
                                    "required": ["yoE", "required", "preferred", "minEducation", "certsLicences", "categorys"],
                                    "additionalProperties": False
                                }
                                },
                                "required": [
                                "calendar_event"
                                ],
                                "additionalProperties": False
                            }
                            }
            }
                }
    }

    return(response)
    
        







