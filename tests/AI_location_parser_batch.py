import json


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
   }
}

category_prop = {
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


def Ai_parse_location_batch(parsable):
    req_field = []

    input_schema = {
        "type": "object",
        "properties": {},
        "additionalProperties": False
    }

    system_parts = []
    user_parts = []

    title = parsable.get("title")
    location = parsable.get("location")
    yoe = parsable.get("yoe")
    description = parsable.get("description")

    if description:
        user_parts.append(description)

    if location:
        input_schema["properties"]["locations"] = locations_field["locations"]
        req_field.append("locations")
        system_parts.append("Extract the job locations in a City, State format. If remote both city and state should be remote.")
        if not description:
            user_parts.append(location)

    if title:
        req_field.append("category")
        input_schema["properties"]["category"] = category_prop
        system_parts.append("Choose the most fitting job category, one must be chosen.")
        user_parts.append(title)

    if yoe:
        input_schema["properties"]["yoe"] = yoe_field["yoe"]
        req_field.append("yoe")
        system_parts.append("Extract the minimum total years of experience required for the role mentioned in the description.")
        if not description:
            user_parts.append(yoe)

    if req_field:
        input_schema["required"] = req_field

    params = {
        "model": "claude-haiku-4-5",
        "max_tokens": 1024,
        "output_config": {
            "format": {
                "type": "json_schema",
                "schema": input_schema
            }
        },
        "messages": [{"role": "user", "content": "\n\n".join(user_parts)}]
    }

    if system_parts:
        params["system"] = " ".join(system_parts)

    return params
