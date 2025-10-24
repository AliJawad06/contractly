schema =   {
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