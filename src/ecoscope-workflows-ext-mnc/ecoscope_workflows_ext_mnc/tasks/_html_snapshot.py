from typing import Annotated, Union, List
from pathlib import Path
from multiprocessing import Pool

from ecoscope_workflows_core.decorators import task
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright, Browser, Page


class ScreenshotConfig(BaseModel):
    width: int = 1280
    height: int = 720
    full_page: bool = False
    device_scale_factor: float = 2.0
    wait_for_timeout: int = 60_000


def _get_output_path(html_path: Path, output_dir: str) -> Path:
    """Constructs the output path for the PNG file."""
    if output_dir.startswith("file://"):
        output_dir = output_dir[7:]
    return Path(output_dir) / html_path.with_suffix(".png").name


def _take_screenshot(page: Page, output_path: Path, config: ScreenshotConfig) -> None:
    """Takes a screenshot of the page."""
    page.screenshot(path=output_path, full_page=config.full_page, timeout=0)


def _setup_page(browser: Browser, config: ScreenshotConfig) -> Page:
    """Sets up a new browser page with the given configuration."""
    page = browser.new_page(
        viewport={"width": config.width, "height": config.height},
        device_scale_factor=config.device_scale_factor,
    )
    return page


def _navigate_and_wait(page: Page, html_path: Path, config: ScreenshotConfig) -> None:
    """Navigates to the HTML file and waits for the page to load."""
    page.goto(html_path.as_uri(), timeout=0)
    page.wait_for_load_state("networkidle", timeout=0)
    page.wait_for_timeout(config.wait_for_timeout)

@task
def _convert_html_to_png(
    html_path: str,
    output_dir: str,
    config: ScreenshotConfig,
) -> str:
    """Helper function with the core conversion logic."""
    html_path = Path(html_path)
    output_path = _get_output_path(html_path, output_dir)

    with sync_playwright() as p:
        with p.chromium.launch() as browser:
            page = _setup_page(browser, config)
            _navigate_and_wait(page, html_path, config)
            _take_screenshot(page, output_path, config)

    return str(output_path)


def _html_to_png_worker(args: tuple[str, str, ScreenshotConfig]) -> str:
    """Worker function for the multiprocessing pool."""
    html_path, output_dir, config = args
    return _convert_html_to_png(html_path, output_dir, config)


@task
def html_snapshot(
    html_path: Annotated[Union[str, List[str]], Field(description="The html file path(s)")],
    output_dir: Annotated[str, Field(description="The output root path")],
    config: Annotated[
        ScreenshotConfig, Field(description="The screenshot configuration")
    ] = ScreenshotConfig(),
) -> Union[str, List[str]]:
    """
    Task to convert a single HTML file or a list of HTML files to PNG images.
    If a list is provided, the conversion is done in parallel using multiprocessing.

    To speed up the process for multiple files, provide a list of html_path.
    This will use a multiprocessing pool to distribute the work across multiple CPU cores,
    significantly reducing the total processing time.
    """
    if isinstance(html_path, str):
        return _convert_html_to_png(html_path, output_dir, config)

    # list of paths, use multiprocessing
    with Pool() as pool:
        args_list = [(path, output_dir, config) for path in html_path]
        output_paths = pool.map(_html_to_png_worker, args_list)
    return output_paths