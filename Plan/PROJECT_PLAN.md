# IST3134 Big Data Analytics — Group Assignment Plan

*One-page memory for "what is this project and what does it produce?" — read this whenever you feel lost.*

---

## 1. The one-sentence version

We analyse **10 years of US flight-delay data** two different ways — the
"normal" way (**pandas on one laptop**) and the "big data" way
(**PySpark on AWS**) — and show that as the data grows, the big-data way keeps
working while the laptop way slows down and eventually chokes.

That contrast **is** the assignment. The flight analysis is the vehicle; the
real point is demonstrating *why* big data tools matter.

---

## 2. The question we answer

**Main question:** Over the ten-year period from 2015 to 2024, which airports,
routes, carriers, and times of day experienced the worst flight delays, and how did
the mix of delay causes shift over the course of the decade?

Because that bundles several distinct things together, we split it into **five
sub-questions**. Each is a complete research question in its own right, and each maps
directly to one output table (see Section 5):

- **Q1 (By carrier):** Which airlines had the worst arrival delays over 2015–2024, measured by their average arrival delay and by the percentage of their flights that were delayed?
- **Q2 (By airport):** Which origin airports had the worst departure performance over 2015–2024, measured by the average delay and the percentage of delayed flights leaving each airport?
- **Q3 (By route):** Which origin-to-destination routes had the worst delays over 2015–2024, among routes with enough flights to be meaningful?
- **Q4 (By time of day):** How does a flight's likelihood and length of delay vary with its scheduled departure hour, and which hours of the day are the worst to fly?
- **Q5 (Cause mix over time):** How has the share of total delay attributable to each of the five causes — carrier, weather, national airspace system, security, and late-arriving aircraft — changed from year to year across the decade?

Q1 through Q4 are "who, what, and when is worst" snapshots of the delay picture,
while Q5 asks how that picture has changed over time.

*Throughout, a flight is counted as "delayed" if its arrival delay is 15 minutes or
more (the standard US Department of Transportation definition).*

## 3. The dataset

Our data comes from the **Airline On-Time Performance** collection published by the
United States **Bureau of Transportation Statistics (BTS)**, available free and without
any login at transtats.bts.gov. It is one of the most complete public records of air
travel anywhere: every domestic flight operated by the major US airlines is reported to
the government, and each one becomes a single row in the dataset.

Each row is a detailed portrait of one flight. It tells you **who flew it** — the
airline, and the specific aircraft and flight number. It tells you **where it went** —
the origin and destination airports, the cities and states they sit in, and the distance
between them. It tells you **when it was meant to happen versus when it actually did** —
the scheduled departure and arrival times set months in advance, alongside the real times
the plane pushed back and touched down, and from the gap between the two, exactly how many
minutes early or late the flight was leaving and arriving. And it tells you **what
happened to the flight** as an event: whether it operated normally, was cancelled
outright, or was diverted to a different airport.

The part that makes this dataset special for a delay study is that it does not just record
*that* a flight was late — it records **why**. When a flight arrives fifteen minutes or
more behind schedule, its lateness is split across five official causes: delay the
**airline itself** caused (crew, maintenance, baggage), **weather**, congestion in the
**national airspace system** (air-traffic control, heavy traffic, airport conditions),
**security**, and **late-arriving aircraft** — the domino effect of the same plane
running late on its previous trip. This breakdown is what lets us ask not only which
flights are worst, but what is actually driving the delays and how that has changed over
time.

For our analysis we lean on a focused set of these fields: the airline, the origin and
destination airports, the scheduled and actual times, the arrival and departure delays,
the cancellation flag, the distance, and the five delay-cause columns.

## 4. How we run it and what we compare

### 4a. The two implementations
We run the exact same analysis two different ways so we can compare them:

- **Local (baseline):** Python + pandas on a single machine (one laptop).
- **Cloud (big data):** PySpark on AWS (EMR cluster reading the data from S3).

### 4b. The durations we compile and experiment with
We run each implementation over four increasing data sizes, so we can watch how each
one behaves as the data grows:

- **1 month** (Jan 2024) — the small "match the numbers" test.
- **1 year** (2024) — first real benchmark.
- **5 years** (2020–2024).
- **10 years** (2015–2024) — full scale.

### 4c. What we measure (beyond the analysis itself)
For every run, on top of producing the flight-delay answers, we record how the tool
*performed* — this is the evidence for the big-data argument:

- **Runtime** — how long the whole run takes (seconds).
- **Peak memory** — the most memory the run used at once (GB).
- **Whether it completes at all** — pandas is expected to slow down and eventually
  run out of memory or fail at the larger durations, while Spark should finish all four.

## 5. What each sub-question produces (table + chart)

Every run — pandas or Spark, at any duration — produces the same set of outputs.
Each sub-question from Section 2 is answered by exactly one result table and one
chart, as follows:

- **Q1 (Carrier).** The output is a table of every airline with its average arrival delay and its percentage of delayed flights, and the chart is a **horizontal bar chart of airlines ranked from worst to best on-time performance**. This shows at a glance which carriers are the most and least reliable.

- **Q2 (Airport).** The output is a table of origin airports (those with at least 1,000 flights) with their average delay and percentage of delayed flights, and the chart is a **horizontal bar chart of the worst airports**, optionally shown on a US map. This shows which airports travellers are most likely to be delayed leaving from.

- **Q3 (Route).** The output is a table of origin-to-destination routes (those with at least 500 flights) ranked by delay, and the chart is a **horizontal bar chart of the worst routes**. This shows which specific city pairs are the most delay-prone.

- **Q4 (Time of day).** The output is a table of the 24 scheduled departure hours with the average delay and percentage delayed for each, and the chart is a **line or bar chart of delay against hour of day (0–23)**. This shows how delays build up through the day and which hours are the worst to fly.

- **Q5 (Cause mix over time).** The output is a table of each year (2015–2024) with the share of total delay minutes contributed by each of the five causes, and the chart is a **stacked area or stacked bar chart across the years**. This shows how the balance between carrier, weather, airspace, security, and late-aircraft delays has shifted over the decade.

Separately from these five, every run also prints a **performance line** — the number
of rows processed, the total runtime, and the peak memory used. These numbers are not
about flights; they feed the sixth output below.

- **The comparison (the big-data point).** Collecting the performance lines from
  pandas and Spark at 1, 5, and 10 years gives us a **runtime-versus-data-size chart
  and a memory-versus-data-size chart**, each with one line for pandas and one for
  Spark. This is the output that demonstrates *why* big data tools are needed: the
  pandas line climbs steeply and eventually fails, while the Spark line stays flat.

## 6. Deliverables (what we hand in)

- **Report** (Word or PDF) containing:
  - Brief introduction to the problem.
  - Brief introduction to the dataset (with the link).
  - Explanation of the Spark / big-data approach.
  - Analysis of the output with justifications.
  - An **individual reflection from each team member**.
- **GitHub repository** with all source code and the dataset link — the grader marks whatever is in the repo, so it must be self-contained and runnable.
- **Zip file** of any extra materials, submitted alongside the report.

**Marking (20% total):** problem intro 10% · dataset intro 10% · Spark approach 20% · output analysis 20% · reflection 20% · code quality 10% · implementation (the two-way comparison) 10%.

## 7. Key dates & milestones

- **24 July 2026** — Deadline to confirm our dataset with the lecturer. We must show a working sample and get approval *before* building at full scale.
- **~3 August 2026** — Submission link opens on eLearn (a week before the deadline).
- **10 August 2026** — Submission window opens.
- **13 August 2026, 11:59 pm** — Final deadline: report + GitHub repo (+ any extras zipped) must be submitted.
