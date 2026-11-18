"""Drive the frontend in a real headless Chrome through the DevTools protocol.

    uv run python scripts/ui_e2e.py          (needs the gateway on :8080, e.g. `make k8s-gateway`)

Acts like a person: offers help, asks for help, waits for the match to show in the tracker, then
opens the Live board and System tabs. Screenshots go to data/shots/ (git-ignored). It leaves one
offer and one matched request behind; cancel/offline them or reset the cluster afterwards."""

import base64
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from websockets.sync.client import connect

ROOT = Path(__file__).resolve().parent.parent / "data"  # git-ignored scratch folder
ROOT.mkdir(exist_ok=True)
SHOTS = ROOT / "shots"
SHOTS.mkdir(exist_ok=True)
PORT = 9333
URL = "http://localhost:8080/"

chrome = subprocess.Popen(
    ["google-chrome", "--headless=new", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
     f"--remote-debugging-port={PORT}", f"--user-data-dir={ROOT / 'chrome-profile'}", "about:blank"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(40):
        try:
            tabs = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json"))
            page = next(t for t in tabs if t["type"] == "page")
            break
        except Exception:
            time.sleep(0.25)
    ws = connect(page["webSocketDebuggerUrl"], max_size=50_000_000)
    _id = 0
    console_errors: list[str] = []

    def cdp(method, **params):
        global _id
        _id += 1
        ws.send(json.dumps({"id": _id, "method": method, "params": params}))
        while True:
            msg = json.loads(ws.recv())
            if msg.get("method") == "Runtime.exceptionThrown":
                console_errors.append(msg["params"]["exceptionDetails"].get("text", "exception"))
            if msg.get("method") == "Runtime.consoleAPICalled" and msg["params"]["type"] == "error":
                console_errors.append(" ".join(str(a.get("value", a.get("description", ""))) for a in msg["params"]["args"]))
            if msg.get("id") == _id:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg.get("result", {})

    def js(expr):
        r = cdp("Runtime.evaluate", expression=expr, returnByValue=True, awaitPromise=True)
        if "exceptionDetails" in r:
            raise RuntimeError(r["exceptionDetails"])
        return r["result"].get("value")

    def shot(name):
        data = cdp("Page.captureScreenshot", format="png")["data"]
        (SHOTS / f"{name}.png").write_bytes(base64.b64decode(data))

    def wait_for(expr, what, timeout=25):
        t0 = time.time()
        while time.time() - t0 < timeout:
            v = js(expr)
            if v:
                return v
            time.sleep(0.4)
        raise AssertionError(f"timed out waiting for: {what}")

    def click_tab(name):
        js(f"document.querySelector('[data-tab={name}]').click()")
        time.sleep(0.6)

    results = []

    def check(name, ok, detail=""):
        results.append((name, ok))
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")

    cdp("Runtime.enable")
    cdp("Page.enable")
    cdp("Emulation.setDeviceMetricsOverride", width=1280, height=900, deviceScaleFactor=1, mobile=False)
    cdp("Page.navigate", url=URL)
    wait_for("document.querySelector('#need-send') !== null", "page loaded")
    time.sleep(2)
    check("page loads, status shows system online", "online" in js("document.body.innerText").lower())

    # --- offer help: "I can help" -------------------------------------------------
    click_tab("help")
    js("""(() => { const s = document.querySelector('[data-loc=help] select'); s.value = '4'; s.dispatchEvent(new Event('change', {bubbles: true}));
         document.querySelector('#help-qty').value = '7'; document.querySelector('#help-name').value = 'UI-test depot'; })()""")
    time.sleep(0.5)
    check("offer button enabled after choosing a place", js("!document.querySelector('#help-send').disabled"))
    js("document.querySelector('#help-send').click()")
    wait_for("document.querySelector('#help-list').innerText.includes('UI-test depot')", "offer shows under 'My offers'")
    check("offer appears under My offers", True)
    shot("ui-help-tab")

    # --- ask for help: "I need help" ----------------------------------------------
    click_tab("need")
    js("""(() => { const s = document.querySelector('[data-loc=need] select'); s.value = '0'; s.dispatchEvent(new Event('change', {bubbles: true})); })()""")
    time.sleep(0.5)
    check("request button enabled after choosing a place", js("!document.querySelector('#need-send').disabled"))
    js("document.querySelector('#need-send').click()")
    wait_for("!document.querySelector('#need-track').hidden", "tracker appears")
    check("tracker appears after requesting help", True)
    text = wait_for("(() => { const t = document.querySelector('#need-track').innerText; return /on the way|No one nearby/i.test(t) ? t : '' })()",
                    "request resolved to ASSIGNED or UNMATCHED")
    matched = "on the way" in text.lower()
    check("request was matched and the tracker says so", matched, "| " + " ".join(text.split())[:150])
    shot("ui-need-tracker")

    # --- other tabs ---------------------------------------------------------------
    click_tab("live")
    time.sleep(1.5)
    live = js("document.body.innerText")
    check("Live board shows requests", "UI-test depot" in live or "Kurla" in live)
    shot("ui-live-board")
    click_tab("system")
    time.sleep(3)
    check("System tab shows the storage cluster", "shard" in js("document.body.innerText").lower()
          or js("document.querySelector('iframe') !== null"))
    shot("ui-system")

    check("no JavaScript errors during the whole session", not console_errors, "; ".join(console_errors[:3]))
    print(f"\n{sum(ok for _, ok in results)}/{len(results)} checks passed")
    sys.exit(0 if all(ok for _, ok in results) else 1)
finally:
    chrome.terminate()
