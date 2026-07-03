import json 

from openai import OpenAI



locations_field = { 
    "locations": {
      "type": "array",
      "description": "List of extracted city/state pairs",
      "items": {
        "type": "object",
        "properties": {
          "city": {
            "type": "string",
            "description": "The extracted city name"
          },
          "state": {
            "type": "string",
            "description": "Two-letter US state abbreviation",
            "enum": [
              "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
            ]
          }
        },
        "required": ["city", "state"],
        "additionalProperties": False
      }
    }
}


yoe_field = {
   "yoe": {
      "type": "integer", 
      "description": "extracted Years of experience",
      "required": "integer", 
      "additionalProperties": False
   }
}

category_prop =  {
          "type": "string",
          "description": "The job or content category",
          "enum": [
            "Healthcare & Medical",
            "Information Technology & Software Development",
            "Engineering & Manufacturing",
            "Business & Finance",
            "Skilled Trades & Construction"
          ]
        }




async def Ai_parse_location(parsable):
    req_field = []  # Move inside function to avoid mutation
    


    req_structure = {
      "type": "object",
      "properties": {
      },
      "additionalProperties": False
    }
    req_input = []
    
    title = parsable.get("title")
    location = parsable.get("location")
    yoe = parsable.get("yoe")
    
    
    if location:
        print(f"location recieved: {location}")
        req_structure["properties"]["locations"] = locations_field["locations"]  # Fixed typo: properites -> properties
        req_field.append("locations")
        req_input.append({"role": "system", "content": "Extract the job locations in a City, State format. If remote both city and state should be remote"})
        req_input.append({"role": "user", "content": f"{location}"})
        req_structure["required"] = req_field

    

    if title:
      req_field.append("category")
      req_structure["properties"]["category"] = category_prop
      req_input.append({"role": "system", "content": "Choose the most fitting job category, one must be chosen"})
      req_input.append({"role": "user", "content": title})
      req_structure["required"] = req_field


    if yoe: 
       req_structure["properties"]["yoe"] = yoe_field["yoe"]
       req_field.append("yoe")
       req_input.append({"role": "system", "content": "extract the minimum total years of experience required for role mentioned in the description"})
       req_input.append({"role": "user", "content": yoe})
       req_structure["required"] = req_field
       



    response = client.chat.completions.create(  # Fixed: responses.parse -> chat.completions.create
        model="gpt-5-mini",
        messages=req_input,  # Fixed: input -> messages
        response_format={  # Fixed: "text" -> response_format
            "type": "json_schema",
            "json_schema": {
                "name": "response_schema",
                "strict": True,
                "schema": req_structure
            }
        }
    )
    
    event = json.loads(response.choices[0].message.content)  # Fixed: output_parsed doesn't exist
    return event  # Added return statement







    

    


    

