if __name__ == "__main__":

    import argparse
    import re
    import json
    import geopy
    from geopy.extra.rate_limiter import RateLimiter    

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest="input")
    parser.add_argument("--output", dest="output")
    args = parser.parse_args()

    locator = RateLimiter(geopy.Nominatim(user_agent="myGeocoder").geocode, min_delay_seconds=2)
    with open(args.input, "rt") as ifd, open(args.output, "wt") as ofd:
        for line in ifd:
            code, name = re.match(r"^(\S+)\s+(.*)$", line).groups()
            print(name)
            try:
                location = locator(name, timeout=10)
                ofd.write(
                    json.dumps(
                        {
                            "code" : code,
                            "name" : name,
                            "geocoded_name" : str(location),
                            "latitude" : location.latitude,
                            "longitude" : location.longitude
                        }
                    ) + "\n"
                )
            except:
                ofd.write(
                    json.dumps(
                        {
                            "code" : code,
                            "name" : name,
                        }
                    ) + "\n"
                )

