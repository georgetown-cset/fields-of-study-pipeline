import pytest

from fos.corpus import Translator


@pytest.mark.skip("cost")
def test_translate_text():
    translator = Translator()
    text = "与乌克兰类似，台湾老一代和年轻一代对家国的记忆和感知相去甚远"
    translation = translator.translate(text)
    assert isinstance(translation, str)


@pytest.mark.skip("cost")
def test_translate_batch():
    translator = Translator()
    texts = [
        "与乌克兰类似，台湾老一代和年轻一代对家国的记忆和感知相去甚远",
        "曼哈顿消失的中文路标",
        "中国严防疫情，全球供应链再受冲击",
    ]
    result = list(translator.translate_batch(texts, batch_size=2))
    assert isinstance(result, list)
    assert len(result) == 2
    assert len(result[0]) == 2
    assert len(result[1]) == 1
    assert isinstance(result[0][0], str)
