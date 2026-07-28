import pytest
from fastapi import HTTPException

from newsbot import api


@pytest.mark.asyncio
async def test_management_auth_rejects_missing_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api.settings, "management_token", "secret")
    with pytest.raises(HTTPException) as error:
        await api.require_management(None)
    assert error.value.status_code == 401
    await api.require_management("Bearer secret")
