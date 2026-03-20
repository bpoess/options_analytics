#!/usr/bin/env python3
"""Parse 'Raw OptionChains response' lines from a log file and summarize the options."""

import argparse
import json
import sys
from pathlib import Path

PATTERN = "Raw OptionChains response: "


def extract_responses(log_path: Path) -> list[dict]:
    results = []
    with open(log_path) as f:
        for line_no, line in enumerate(f, 1):
            idx = line.find(PATTERN)
            if idx == -1:
                continue
            json_str = line[idx + len(PATTERN) :].strip()
            try:
                results.append(json.loads(json_str))
            except json.JSONDecodeError as e:
                print(
                    f"Warning: failed to parse JSON on line {line_no}: {e}",
                    file=sys.stderr,
                )
    return results


def summarize(data: dict) -> None:
    near_price = data.get("nearPrice")
    pairs = data.get("OptionPair", [])

    options: list[tuple[str, float, dict]] = []
    for pair in pairs:
        for side in ("Call", "Put"):
            if side in pair:
                opt = pair[side]
                strike = opt.get("strikePrice", 0.0)
                options.append((side, strike, opt))

    options.sort(key=lambda x: (x[1], x[0]))

    if not options:
        print("No options found in response.")
        return

    # Derive underlying and expiry from first option description
    # Format: "TSLA Mar 20 '26 $5 Call"
    first = options[0][2].get("displaySymbol", "")
    parts = first.split("$")[0].strip()
    print(f"Chain: {parts}")
    if near_price is not None:
        print(f"Near price: ${near_price:.2f}")

    print()
    header = (
        f"  {'Type':4s}  {'Strike':>8s}  {'Bid':>8s}  {'Ask':>8s}"
        f"  {'Last':>8s}  {'Chg':>8s}  {'Vol':>7s}  {'OI':>7s}"
        f"  {'ITM':>3s}"
        f"  {'IV':>7s}  {'Delta':>7s}  {'Gamma':>7s}"
        f"  {'Theta':>7s}  {'Vega':>7s}  {'Rho':>7s}"
        f"  Description"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    for side, strike, opt in options:
        greeks = opt.get("OptionGreeks", {})
        itm = opt.get("inTheMoney", "")
        print(
            f"  {side:4s}"
            f"  {strike:8.2f}"
            f"  {opt.get('bid', 0):8.2f}"
            f"  {opt.get('ask', 0):8.2f}"
            f"  {opt.get('lastPrice', 0):8.2f}"
            f"  {opt.get('netChange', 0):+8.2f}"
            f"  {opt.get('volume', 0):7d}"
            f"  {opt.get('openInterest', 0):7d}"
            f"  {itm:>3s}"
            f"  {greeks.get('iv', 0):7.4f}"
            f"  {greeks.get('delta', 0):7.4f}"
            f"  {greeks.get('gamma', 0):7.4f}"
            f"  {greeks.get('theta', 0):7.4f}"
            f"  {greeks.get('vega', 0):7.4f}"
            f"  {greeks.get('rho', 0):7.4f}"
            f"  {opt.get('displaySymbol', '')}"
        )

    n_calls = sum(1 for s, _, _ in options if s == "Call")
    n_puts = sum(1 for s, _, _ in options if s == "Put")
    strikes = sorted({strike for _, strike, _ in options})
    print(f"\nTotal: {len(options)} options ({n_calls} calls, {n_puts} puts)")
    print(f"Strikes: {len(strikes)} unique, ${strikes[0]:g} – ${strikes[-1]:g}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_file", type=Path, help="Path to the log file")
    parser.add_argument(
        "-n",
        type=int,
        default=0,
        help="Which entry to show (1-based, negative counts from end; 0 = all)",
    )
    args = parser.parse_args()

    responses = extract_responses(args.log_file)
    if not responses:
        print("No 'Raw OptionChains response' lines found.", file=sys.stderr)
        sys.exit(1)

    if args.n == 0:
        entries = list(enumerate(responses, 1))
    else:
        try:
            idx = args.n - 1 if args.n > 0 else args.n
            entries = [(args.n if args.n > 0 else len(responses) + args.n + 1, responses[idx])]
        except IndexError:
            print(
                f"Entry {args.n} out of range (found {len(responses)} entries)",
                file=sys.stderr,
            )
            sys.exit(1)

    for i, (num, data) in enumerate(entries):
        if i > 0:
            print("\n" + "=" * 60 + "\n")
        print(f"Entry {num}/{len(responses)}")
        print("-" * 40)
        summarize(data)


if __name__ == "__main__":
    main()
