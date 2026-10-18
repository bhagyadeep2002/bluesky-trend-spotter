# Dashboard QA inventory

The dashboard is a scrollable observation tool. At desktop widths the initial
viewport must show its identity, demo/live mode, pipeline metrics and emerging
topic entry. At 390px the initial viewport must show identity, mode and metrics;
the ranked list and evidence remain reachable through normal page scrolling.

| Claim / control | Functional check | Visual evidence |
| --- | --- | --- |
| Demo is explicit | Visible demo banner and pipeline mode | Desktop overview and mobile |
| Rankings use actual application data | Aurora shows 24 posts, 4 baseline and 4.3× growth | Overview |
| Refresh updates the view | Click Refresh and wait for response | Overview after click |
| Topic link exposes evidence | Click #aurora, inspect count, timeline and terms | Topic detail |
| Timeline is readable and accessible | Chart present, expand data table | Detail and mobile |
| Evidence pagination works | Next, Previous and their disabled states | Detail, evidence list |
| Search available posts | Search aurora; then a nonexistent term | Overview search states |
| Loading / error / empty states | Initial load, failed request + Refresh recovery, empty search | Error screenshot |
| Invalid topic handled | Visit missing topic, see error and return | Error state |
| Mobile fits | 390×844 screenshots and horizontal overflow check | Mobile overview/detail |
| Source text stays text | Inject script-like evidence in a test response; no execution | Browser assertion |
| Deleted evidence stays absent | API/runtime tests delete records and fetch evidence/search | Backend test suite |
| All visible navigation works | Overview, back link and API docs | Browser assertions |

Browser tests run against the explicitly seeded demo only. Screenshots must
never capture or commit live post text. In-app Browser discovery returned no
available connections; local Playwright Chromium is used for standalone QA.
Automated script: `npm ci && npx playwright install chromium`, then run the demo
server and `npm run test:ui`. Generated images go into ignored `test-results/`.

Signoff: all scripted controls and recovery checks passed on 2026-10-18.
Reviewed overview/detail screenshots at 1440×1000 and mobile at 390×844,
including full-page evidence, timeline and error states. Fixed a clipped
decorative wave and overlapping mobile time labels. No horizontal page overflow,
script execution, uncaught page errors, text clipping or obscured controls were
found in the reviewed final states. The app intentionally scrolls vertically.
Browser contexts/processes are closed by the QA script. Selected synthetic-only
screenshots are in docs/screenshots. Optional live user-content screenshots are
excluded. Native desktop window geometry is irrelevant to this web dashboard.
