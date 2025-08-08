from pydantic import BaseModel, Field
from typing import Optional

class PokeRequest(BaseModel):

    id: Optional[int] = Field(
        default=None,
        ge = 1,
        description = "ID de la petición",
    )

    pokemon_type: Optional[str] = Field(
        default=None,
        description="Tipo de pokémon a solicitar",
        pattern="^[a-zA-Z0-9_]+$"
    )

    url: Optional[str] = Field(
        default=None,
        description="URL de la petición",
        pattern="^https?://[^\s/$.?#].[^\s]*$"
    )

    status: Optional[str] = Field(
        default=None,
        description="Estado de la petición",
        pattern="^(sent|inprogress|deliver|failed)$"
    )

    sample_size : Optional[int] = Field(
        default=None,
        description="Size of random pokemon data",
        ge=0
    )