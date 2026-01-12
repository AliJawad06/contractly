from workers import Response, WorkerEntrypoint
import json
from urllib.parse import urlparse
from workers import Response, WorkerEntrypoint
from urllib.parse import urlparse, parse_qs

def sanitize(v):
    if v is None:
        return None

    if str(type(v)) == "<class 'pyodide.ffi.JsProxy'>":
        try:
            if v.to_py() is None:
                return None
        except:
            return None

    if isinstance(v, str):
        v = v.strip()
        return v if v else None

    if isinstance(v, (int, float)):
        return v

    if isinstance(v, bool):
        return 1 if v else 0

    return None


class Default(WorkerEntrypoint):

    async def on_fetch(self, request):

        url = urlparse(request.url)
        pathname = request.url.split("?")[0]

        if pathname.endswith("/api/jobs"): 
            if request.method != "POST":
                return Response("POST only", status=405)

            raw = await request.text()
            jobs = json.loads(raw)

            query = """
            INSERT INTO jobs (
            title, category, location, posted, url,
            postedStr, startStr, start_ts,
            description, experience, remote
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            inserted = 0
            failed = 0

            for job in jobs:
                try:
                    await self.env.contractme_db.prepare(query).bind(
                        sanitize(job.get("title")),
                        sanitize(job.get("category")),
                        sanitize(job.get("location") or "N/A"),
                        sanitize(job.get("posted")),
                        sanitize(job.get("url")),
                        sanitize(job.get("postedStr")),
                        sanitize(job.get("startStr") or "N/A"),
                        sanitize(job.get("start") or -1),  
                        sanitize(job.get("description")),
                        sanitize(job.get("experience")),
                        sanitize(job.get("remote"))
                    ).run()

                    inserted += 1

                except Exception as e:
                    failed += 1
                    print("FAILED:", job.get("title"))
                    print("ERROR:", str(e))

            return Response.json({"inserted": inserted, "failed": failed})
        

        
        if url.path == "/api/get_jobs":
            params = parse_qs(url.query)

            # ✅ Extract all category values (array)
            categories = params.get("category", [])
            
            if not categories:
                # No categories provided, return all jobs
                stmt = self.env.DB.prepare("SELECT * FROM jobs")
                result = await stmt.run()
                return Response.json(result.results)
            
            # Build IN clause for multiple categories
            placeholders = ','.join(['?' for _ in categories])
            query = f"SELECT * FROM jobs WHERE category IN ({placeholders})"
            
            stmt = self.env.DB.prepare(query).bind(*categories)
            result = await stmt.run()

            return Response.json(result.results)



