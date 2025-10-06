

#minimum education: HighSchool, Associates, Bachelors, Masters PHd. 
#minimum years of experience

from bs4 import BeautifulSoup
from openai import OpenAI


from pydantic import BaseModel
from typing import List, Dict, Optional

#minEducation: HS Diploma, Associate's, Bachelor's, Masters, PHD

class Skills(BaseModel):
    hardSkills: List[str]
    softSkills: List[str]

class CalendarEvent(BaseModel):
    yoE: int
    required: Skills
    preferred: Skills
    minEducation: str
    certifications: List[str]


async def getSkills(job):
     description = job['description']
     if not description:
        return ""
     soup = BeautifulSoup(description, "html.parser")
     description = soup.get_text(separator=" ", strip=True)
     client = OpenAI()

     response = client.responses.create(
         model="gpt-5",
         input="Write a short bedtime story about a unicorn."
      )

     print(response.output_text)





                        