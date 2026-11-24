"""Display the actual SonarCloud gate and open vulnerabilities for the filmed branch."""
import argparse
import base64
import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def query(endpoint, **args):
    req = Request("https://sonarcloud.io/api/" + endpoint + "?" + urlencode(args))
    if token := os.getenv("SONAR_TOKEN"):
        req.add_header("Authorization", "Basic " + base64.b64encode((token + ":").encode()).decode())
    with urlopen(req, timeout=20) as response:
        return json.load(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project")
    parser.add_argument("--branch")
    args = parser.parse_args()
    scope = {"branch": args.branch} if args.branch else {}
    gate = query("qualitygates/project_status", projectKey=args.project, **scope)["projectStatus"]
    print("Quality Gate:", gate["status"])
    for c in gate["conditions"]:
        print(c["metricKey"], c.get("actualValue"), c["status"])
    page = 1
    while True:
        result = query("issues/search", componentKeys=args.project, types="VULNERABILITY",
                       resolved="false", p=page, ps=100, **scope)
        for issue in result["issues"]:
            print(issue["key"], issue["rule"], issue["component"], issue.get("line"), issue["message"])
        if page * 100 >= result["paging"]["total"]:
            break
        page += 1
