import argparse
import json
import os


def get_parser():
    parser = argparse.ArgumentParser(
        description="Split a start_year/stop_year/start_month/stop_month search range into "
                    "shorter periods, so each period's batch_image_correlation matrix job "
                    "stays under GitHub's 256-job matrix limit"
    )
    parser.add_argument("start_year", type=str)
    parser.add_argument("stop_year", type=str)
    parser.add_argument("start_month", type=str)
    parser.add_argument("stop_month", type=str)
    parser.add_argument("chunk_mode", type=str, choices=["year", "half_year"])
    return parser


def month_range(start_month, stop_month):
    if start_month <= stop_month:
        return list(range(start_month, stop_month + 1))
    # wrap-around case (e.g., Nov-Feb)
    return list(range(start_month, 13)) + list(range(1, stop_month + 1))


def main():
    args = get_parser().parse_args()
    start_year = int(args.start_year)
    stop_year = int(args.stop_year)
    start_month = int(args.start_month)
    stop_month = int(args.stop_month)
    wraps = start_month > stop_month

    if wraps and args.chunk_mode == "half_year":
        print("start_month > stop_month (wrap-around season); half_year splitting is "
              "not supported for wrap-around ranges, falling back to year chunks")

    periods = []
    for year in range(start_year, stop_year + 1):
        if args.chunk_mode == "half_year" and not wraps:
            months = month_range(start_month, stop_month)
            mid = (len(months) + 1) // 2
            halves = [months[:mid], months[mid:]]
            halves = [half for half in halves if half]
        else:
            # wrap-around seasons are kept whole: splitting Nov-Feb into two halves
            # within a single calendar year would separate images that need to be
            # paired across the Dec/Jan boundary
            halves = [month_range(start_month, stop_month)]

        for months in halves:
            periods.append({
                "start_year": str(year),
                "stop_year": str(year),
                "start_month": str(months[0]),
                "stop_month": str(months[-1]),
                "name": f"{year}_{months[0]:02d}-{months[-1]:02d}",
            })

    matrixJSON = f'{{"include":{json.dumps(periods)}}}'
    print(f"number of periods: {len(periods)}")
    print(json.dumps(periods, indent=2))

    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        print(f"MATRIX={matrixJSON}", file=f)


if __name__ == "__main__":
    main()
