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
    from urllib.error import HTTPError
    try:
        with urlopen(req, timeout=20) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"SonarCloud {endpoint} HTTP {exc.code}: {exc.read().decode()[:500]}") from exc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("project")
    scope_args = parser.add_mutually_exclusive_group()
    scope_args.add_argument("--branch")
    scope_args.add_argument("--pull-request")
    parser.add_argument("--organization", default="mayk-itds")
    args = parser.parse_args()
    scope = {"branch": args.branch} if args.branch else {}
    if args.pull_request:
        scope = {"pullRequest": args.pull_request}
    gate = query("qualitygates/project_status", projectKey=args.project, **scope)["projectStatus"]
    print("Quality Gate:", gate["status"])
    for c in gate["conditions"]:
        print(c["metricKey"], c.get("actualValue"), c["status"])
    page = 1
    while True:
        result = query("issues/search", projects=args.project, organization=args.organization, p=page, ps=100, **scope)
        for issue in result["issues"]:
            print(issue["key"], issue["rule"], issue["component"], issue.get("line"), issue["message"])
        if page * 100 >= result["paging"]["total"]:
            break
        page += 1


if __name__ == "__main__":
    main()
