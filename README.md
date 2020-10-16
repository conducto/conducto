# Conducto

- Website: [conducto.com](https://www.conducto.com)
- Docs:
  - [Hello World](https://www.conducto.com/docs/getting-started/hello-world)
  - [CI/CD](https://www.conducto.com/docs/cicd)
  - [Concepts](https://www.conducto.com/docs/bascs)
- [Demo Video](https://youtu.be/4h8hxCOnM-8)
- [Example Code](https://github.com/conducto/examples)

## Conducto is a CI/CD tool

Here are some things that set it apart:

- Use Python instead of YAML (Javascript API coming soon).
- Explore a tree (not a DAG).
- Fix with local editors, [debug](https://www.conducto.com/docs/basics/debugging#debugging-live-code) in local containers.
- Move workloads between `--local` and `--cloud` with a single argument.

## Computational Pipelines

Consider a computational task that breaks into pieces (and maybe its pieces have pieces too).

Conducto organizes those pieces into trees that we call pipelines.
At [conducto.com](https://conducto.com) we host pages that let you view and control them.

![Pipeline Screenshot](https://github.com/conducto/conducto/blob/main/tree_screenshot.png?raw=true)

## Centralize Insights, Work Wherever

Conducto is good for those long running tasks that people like to check in on.
It provides a place where they can watch them happen (even if they're running on your dev box).

Conducto pipelines are fully encapsulated in containers, so they're portable.
All you need is a Docker, a web browser, and a Conducto account (which is free).

The only paid feature is [cloud mode](https://www.conducto.com/docs/basics/local-vs-cloud), which is where we provide the compute power instead of leaning on your local Docker daemon.

## Try It

You'll need Python 3.6 or higher and [Docker](https://docs.docker.com/get-docker/).
Use pip to install:

    pip install conducto

### A Sample Pipeline

Start with a script like this:

    # pipeline.py
    import conducto as co

    py_code = 'print("Hello")'
    js_code = 'console.log("World!")'

    def pipeline() -> co.Serial:
        root = co.Serial()
        root["hello from python"] = co.Exec(f"python -c '{py_code}'")
        root["world from javascript"] = co.Exec(f"echo '{js_code}' | node -",
                                                image="node:current-alpine")
        return root

    if __name__ == "__main__":
        co.main(default=pipeline)

Launch it like this:

    python pipeline.py --local

You'll be prompted to create a Conducto account, and then you'll see your pipeline.

## Contact

If you want to chat, [join us on Slack](https://join.slack.com/t/conductohq/shared_invite/zt-co82g0up-5HiC6lptzhnhgyPGmKvA3Q).

If you run into problems feel free to use the `conducto` tag on Stack Overflow or file a GitHub issue on this repo.
