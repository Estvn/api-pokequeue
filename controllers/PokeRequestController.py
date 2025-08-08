import json
import logging # Para manejo de logs | Crear un logger
from utils.ABlob import ABlob

from fastapi import HTTPException
from models.PokeRequest import PokeRequest
from utils.database import execute_query_json
from utils.AQueue import AQueue

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def insert_poke_request(poke_request: PokeRequest) -> dict:

    if poke_request.sample_size:
        sample_size = poke_request.sample_size
    else:
        sample_size = 0

    try: 
        query = "exec pokequeue.create_poke_request ?, ?"
        params = (poke_request.pokemon_type, sample_size)
        
        result = await execute_query_json(query, params, True) 
        result_list = json.loads(result)

        await AQueue().insert_message_on_queue(result)
        return result_list 

    except Exception as e:
        logger.error(f"Error inserting poke request: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


async def update_poke_request(poke_request: PokeRequest) -> dict:

    try: 
        query = "exec pokequeue.update_poke_request ?, ?, ?"

        if not poke_request.url:
            poke_request.url = ""

        params = ( poke_request.id, poke_request.status, poke_request.url )
        result = await execute_query_json( query, params, True ) # True para el commit 
        result_dict = json.loads(result)

        return result_dict

    except Exception as e:
        logger.error(f"Error updating poke request: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


async def select_poke_request( id: int ):

    try: 
        query = "select * from pokequeue.requests where id = ?"
        params = ( id, )

        result = await execute_query_json( query, params ) 
        result_dict = json.loads(result)

        return result_dict

    except Exception as e:
        logger.error(f"Error selecting poke request: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

async def get_all_request() -> dict:
    query = """
        select
            r.id as ReportId,
            s.description as Status,
            r.type as PokemonType,
            r.url,
            r.created,
            r.updated,
            r.sample_size
        from pokequeue.requests r
        inner join pokequeue.status s on r.id_status = s.id
        where active = 1
        """
    result = await execute_query_json(query)
    result_dict = json.loads(result)
    blob = ABlob()
    for record in result_dict:
        id = record["ReportId"]
        record["url"] = f"{record['url']}?{blob.generate_sas(id)}"
    return result_dict


async def delete_poke_request(id_report: int) -> dict:

    report_response = await select_poke_request(id_report)
    try:
        if report_response[0]["id"]:

            query = "EXEC pokequeue.delete_poke_request ?"
            params = (id_report,)

            result = await execute_query_json( query, params, True ) 
            result_dict = json.loads(result)

            if result_dict[0]["response"] == 1:
                blob = ABlob()
                deleted_csv = blob.delete_csv_from_folder(id_report)

                if deleted_csv:
                    return result_dict[0]
                else:
                    logger.error(f"csv tried to delete was not found.")
                    raise HTTPException(status_code=404, detail="archive was not found")
            
            elif result_dict[0]["response"] == 0:
                logger.error(f"database couldn't delete a value already deleted.")
                raise HTTPException(status_code=404, detail="Value was already deleted")
        
        else:
            logger.error(f"Poke ID doesn't exist")
            raise HTTPException(status_code=404, detail="Value was not found")
    
    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.error(f"Error deleting poke request: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
        
