# ISIC Archive CLI 

A Command Line Interface (CLI) tool that creates a wrapper around the ISIC archive public API.

<pre>
Version: 0.1.0 (pre-release)
</pre>

In pre-release stages, currently can perform the following tasks: 
- Download image metadata.
- Download batches of images.
- Unzip images into a single directory.

## Warranty

This tool comes with **No** warranty of any kind and is used at the user's own risk.

The tool is a work-in-progress and has not been stress tested for corner cases. 

## Usage

### Help 

The tool provides a *--help* switch for all commands to explain its operation and its options parameters.

Use the *--help* switch to get a brief description of what each command does.

From the command line:

```shell script
python cli.py image --help

python cli.py image metadata --help

python cli.py image download --help

python cli.py image unzip --help
```



