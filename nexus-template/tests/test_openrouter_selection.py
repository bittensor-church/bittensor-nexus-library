# pyright: basic

from nexus.actors.openrouter_selection import (
    FileSelection,
    ImageUrlSelection,
    InputAudioSelection,
    ScalarSelection,
    VideoUrlSelection,
)


def test_scalar_selection_renders_final_text_dict() -> None:
    assert ScalarSelection(value="a").render_openrouter_content(index=0, field_name="task_result_id") == [
        {"type": "text", "text": "item[0].task_result_id: a"}
    ]


def test_image_url_selection_renders_label_then_image_dict() -> None:
    assert ImageUrlSelection(url="https://example.com/a.png").render_openrouter_content(
        index=0,
        field_name="image",
    ) == [
        {"type": "text", "text": "item[0].image:"},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
    ]


def test_file_selection_renders_label_then_file_dict() -> None:
    assert FileSelection(
        filename="notes.txt",
        file_data="data:text/plain;base64,SGVsbG8=",
    ).render_openrouter_content(index=0, field_name="attachment") == [
        {"type": "text", "text": "item[0].attachment:"},
        {
            "type": "file",
            "file": {
                "filename": "notes.txt",
                "file_data": "data:text/plain;base64,SGVsbG8=",
            },
        },
    ]


def test_input_audio_selection_renders_label_then_audio_dict() -> None:
    assert InputAudioSelection(data="UklGRg==", format="wav").render_openrouter_content(
        index=0,
        field_name="audio",
    ) == [
        {"type": "text", "text": "item[0].audio:"},
        {
            "type": "input_audio",
            "input_audio": {
                "data": "UklGRg==",
                "format": "wav",
            },
        },
    ]


def test_video_url_selection_renders_label_then_video_dict() -> None:
    assert VideoUrlSelection(url="https://example.com/demo.mp4").render_openrouter_content(
        index=0,
        field_name="video",
    ) == [
        {"type": "text", "text": "item[0].video:"},
        {"type": "video_url", "video_url": {"url": "https://example.com/demo.mp4"}},
    ]
