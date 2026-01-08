import json 
import csv 
import csv
import json

input_csv = '/Users/aj/Downloads/DhikrUp/contractJobs/test_split.json'
output_jsonl = 'base.jsonl'

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

lines = []
# Read the CSV file
with open(input_csv, 'r', encoding='utf-8') as f, open("./eval.jsonl", 'w') as w:
    rows = json.load(f)
    
    for job in rows:
        id, description = job

        content_text = json.loads(job[id])["calendar_event"]
        content_text = json.dumps(content_text, ensure_ascii=False, indent=2)

        line = {
            "messages": [
                { "role": "system", "content":  instructions },
                { "role": "user", "content":  job[description] },
                {"role": "assistant", "content": content_text}
        ]
        }


        w.write(json.dumps(line, ensure_ascii=False) + "\n")





  
    


        


        