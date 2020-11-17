from datetime import datetime, timedelta, timezone
import dateutil.parser
import json
import re
import typing

import conducto as co
from conducto.shared import async_utils, parse_utils


async def cleanup(label, age, clean_networks=True):
    image_ids = await _get_image_ids(label)
    last_use_times = await _get_last_use_times(image_ids)
    age_seconds = parse_utils.duration_string(age)
    cutoff = datetime.now().astimezone(timezone.utc) - timedelta(seconds=age_seconds)

    to_delete = [img for img, tm in last_use_times.items() if tm < cutoff]

    print("Deleting unused images:", to_delete)

    if to_delete:
        # We convert to repo tags because when you push an image to another
        # repository, delete will only delete the image by id with -f switch.
        stdout, stderr = await async_utils.run_and_check(
            "docker", "inspect", *to_delete, "--format={{json .RepoTags}}"
        )

        to_delete_tags = []
        for line in stdout.decode("utf8").split("\n"):
            if line.strip() == "":
                continue
            to_delete_tags += json.loads(line.strip())

        # Set stop_on_error=False because some images cannot be deleted and that's okay.
        # This can happen if some containers are still running, or if they're basic images
        # like 'python' or 'alpine' that are used by images outside of this label.
        if to_delete_tags:
            stdout, stderr = await async_utils.run_and_check(
                "docker", "image", "rm", *to_delete_tags, stop_on_error=False
            )

        # if images are not tagged and match the label, remove them this way
        await async_utils.run_and_check(
            "docker", "image", "prune", "--filter", f"label={label}", "--force"
        )

    if clean_networks:
        await async_utils.run_and_check(
            "docker",
            "network",
            "prune",
            "--filter",
            "label=conducto",
            "--filter",
            "until=10m",
            "-f",
        )


async def _get_image_ids(label):
    out, _err = await async_utils.run_and_check(
        "docker", "image", "ls", "--format", "{{.ID}}", "--filter", f"label={label}"
    )
    return out.decode().splitlines()


async def _get_last_use_times(image_ids) -> typing.Dict[str, datetime]:
    if not image_ids:
        return {}
    out, _err = await async_utils.run_and_check(
        "docker",
        "image",
        "inspect",
        "--format",
        "{{.Metadata.LastTagTime}}",
        *image_ids,
    )
    output = {}
    for image_id, tm_str in zip(image_ids, out.decode().splitlines()):
        try:
            dt = dateutil.parser.parse(tm_str)
        except ValueError:
            # Sometimes Docker gives a string like this, that dateutil can't parse:
            #     2020-04-17 14:51:02.123456789 +0000 UTC
            # Handle this case specially.
            without_nanos = re.sub(r"\.\d+", "", tm_str)
            dt = datetime.strptime(without_nanos, "%Y-%m-%d %H:%M:%S %z %Z")

        output[image_id] = dt.astimezone(timezone.utc)
    return output


def _size_mb(s):
    if s.endswith("GB"):
        return float(s[:-2]) * 1000
    if s.endswith("MB"):
        return float(s[:-2])
    if s.endswith("kB"):
        return float(s[:-2]) * 0.001
    if s.endswith("B"):
        return float(s[:-1]) * 0.001 * 0.001


async def _image_list_sizes(images):
    overlaps = []
    singles = {}
    for img_id in images:
        args = ["docker", "history", img_id, "--format", "{{json .}}"]
        out, _err = await async_utils.run_and_check(*args)

        sizes = []
        for line in out.decode().splitlines():
            if line.strip() == "":
                continue
            obj = json.loads(line)
            layer = [obj["ID"], _size_mb(obj["Size"])]
            if layer[0] == "\u003cmissing\u003e":
                sizes[-1][1] += layer[1]
            else:
                sizes.append(layer)

        overlaps.extend(sizes)
        singles.update(dict(sizes))

    image_virtual_total = sum([v for k, v in overlaps])
    image_size_net = sum([v for k, v in singles.items()])

    return image_virtual_total, image_size_net


async def show_usage(label):
    image_ids = await _get_image_ids(label)

    virtual, size_net = await _image_list_sizes(image_ids)

    print(f"Images labeled {label}:  {len(image_ids)}")
    print(f"Virtual Total:  {virtual:.2f} MB")
    print(f"Net Size:  {size_net:.2f} MB")


if __name__ == "__main__":
    co.main(default=cleanup)
