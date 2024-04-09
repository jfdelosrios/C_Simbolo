# C_Simbolo

Libreria con un conjunto de funciones para obtener información de los simbolos de Binance.

## Archivos requeridos:

* [varios.py](https://github.com/jfdelosrios/varios_python.git)


## Contenido

### Función leerDataFrame

def leerDataFrame(_path:str) -> dict[list[str,str], DataFrame]:
    """Lee un dataFrame del directorio _path."""

### Función BuscarDataFrameSimbolo
def BuscarDataFrameSimbolo(
        _simbolo:str, 
        _timeFrame:str, 
        _ini:int,
        _cantBarras:int,
        _descargar:bool,
        _path:str,
        _espera:float
    ) -> dict[list[str, str], DataFrame]:
    """
    Busca dataframe de binance en en un cierto path:
    * _simbolo: simbolo del dataFrame.
    * _timeframe: TimeFrame del dataFrame.
    * _ini: Fecha mas cercana mayor a la fecha de la barra ubicada en el
                    indice 0,
    * _cantBarras: Cantidad de barras hacia atras que va a descargar
    * _descargar: Si esta en true descarga de la pagina web. 
    * _path: donde se guarda el dataFrame que se descarga de binance.
    * _espera: segundos que espera el programa para hacer una petición
    """


### Clase class C_Simbolo:
    """
    Clase que representa un simbolo determinado. En la practica es como si 
    tuviera dos constructores:

    __init__(monedaBase:str, monedaCotizada:str) -> None

    __init__(simbolo:str) -> None

    Si el simbolo no existe dispara la excepcion 
    binance.exceptions.BinanceAPIException

    Cualquier informacion que requiriese buscar en:
    
    https://python-binance.readthedocs.io/en/latest/

    https://binance-docs.github.io/apidocs/spot/en/

    hay que poner esto en las excepciones cuando se intenta crear el objeto    
        from requests.exceptions import ReadTimeout, ConnectTimeout
        from binance.exceptions import BinanceAPIException
    """ 

####    Metodo __init__(self, **kwargs) -> None:
        ""
        Clase que representa un simbolo determinado. En la practica es como si 
        tuviera dos constructores:

        __init__(monedaBase:str, monedaCotizada:str) -> None

        __init__(simbolo:str) -> None

        Si el simbolo no existe dispara la excepcion 
        binance.exceptions.BinanceAPIException

        Cualquier informacion que requiriese buscar en:
        
        https://python-binance.readthedocs.io/en/latest/

        https://binance-docs.github.io/apidocs/spot/en/#
        """

####    Metodo Simbol_info(self) -> dict:
        """ """
        return self.simbol_info


####    Metodo get_monedaBase(self) -> str:
Retorna la moneda base del simbolo.


####    Metodo get_monedaCotizada(self) -> str:
Retorna la moneda cotizada del simbolo.


####    Metodo Name(self) -> str:
Retorna el nombre del simbolo.


####    Metodo Refresh(self) -> None:
Refresca los datos del simbolo.
    
####    Metodo Order_book(self) -> dict:
        """Nivel 2"""

        self.Refresh()
        return self.order_book

    
####    Metodo Ask(self) -> float:
        """Retorna el precio ask mas bajo.

    
####    Metodo Bid(self) -> float:
        """ """

        self.Refresh()
        return float(self.order_book['bids'][0][0])


####    Metodo avg_price(self) -> dict:
        """ """
        
        return self.client.get_avg_price(symbol = self.simbolo)


####    Metodo point(self) -> float:
        """valor del punto."""

        for i in self.Simbol_info()['filters']:

            if(i['filterType'] != 'PRICE_FILTER'):
                continue

            return float(i['tickSize'])

        return -1


####    Metodo digits(self) -> int:
Retorna la cantidad de digitos decimales del precio. 


####    Metodo formatear_precio(self, precio:float) -> dict:
        """ """

        precision = self.digits()

        for i in self.simbol_info['filters']:

            if(i['filterType'] != 'PRICE_FILTER'):
                continue

            k = (precio - float(i['minPrice'])) % float(i['tickSize'])

            precio = precio - k

            precio = floor2(precio, precision)

            return {'status': ['ok', ''], 'out': precio}

        return {'status': ['error', 'no encontro PRICE_FILTER'], 'out': -1}

        
####    Metodo lote_stepSize(self) -> int:
        """cantidad de digitos decimales del lote."""

        for i in self.Simbol_info()['filters']:

            if(i['filterType'] != 'LOT_SIZE'):
                continue

            string = i['stepSize']
            string = string.rstrip('0')
            return (len(string) - string.find('.') - 1)

        return -1


####    Metodo formatear_cant_monedas(self, cant:float) -> dict:
        """Formatea la cantidad de monedas a comprar."""

        precision = self.lote_stepSize()

        for i in self.simbol_info['filters']:

            if(i['filterType'] != 'LOT_SIZE'):
                continue

            k = (cant - float(i['minQty'])) % float(i['stepSize'])

            cant = cant - k

            cant = floor2(cant, precision)

            return {'status': ['ok', ''], 'out': cant}

        return {'status': ['error', 'no encontro LOT_SIZE'], 'out': -1}


####    Metodo descargarDataFrameBinance(
            self,
            _timeFrame:str,
            _ini:int,
            _cantBarras:int,
            _path:str,
            _espera:float
        ) -> dict[list[str,str]]:
        """
        Descarga un dataFrame de velas de cierto timeFrame (_timeFrame)
        en el directorio _path.

        Entradas:
            
            * _timeFrame: TimeFrame del simbolo a descargar,
            * _ini: Fecha mas cercana mayor a la fecha de la barra ubicada en el
                    indice 0,
            * _cantBarras: Cantidad de barras hacia atras que va a descargar
            * _path: Ruta donde sera guardado el archivo csv,
            * _espera: segundos que espera el programa para hacer una petición
        """

        dataFrameVelas = self.generarDF(
                _interval = _timeFrame,
                _ini = _ini,
                _cantBarras = _cantBarras,
                _espera = _espera
            )

        if(dataFrameVelas['status'][0] == 'error'):
            return {'status': dataFrameVelas['status']}

        makedirs(_path, exist_ok = True)

        dataFrameVelas['out'].to_csv(
                '{}\\{}-{}.csv'.format(_path, self.simbolo, _timeFrame)
            )
        
        return {'status': ['ok', '']}


####    Metodo scrapearTickers(
            self,
            _interval:str, 
            _ini:int, 
            cantBarras:str, 
            _espera:float
        ):
        """
        Usa web scraping para obtener tickers de cierto timeFrame. Donde:

           * _interval: TimeFrame a consultar,

           * _ini: Fecha mas cercana mayor a la fecha de la barra ubicada en el
                    indice 0,

           * cantBarras: Cantidad de barras hacia atras que va a descargar 

           * _espera: segundos que espera el programa para hacer una petición
        """

        candles = []  
        primera = True

        pbar = InitBar()

        totalBarras = cantBarras

        while True:

            if(cantBarras >= 1000):
                    
                sleep(_espera)
                            
                try:
                            
                    candles_tpm = self.client.get_klines(
                            symbol = self.simbolo, 
                            interval = _interval, 
                            limit = 1000,
                            endTime = _ini
                        )
                    
                except BinanceAPIException as error:
                    del pbar 

                    return {'status': ['error', str(error)], 'out': candles}

                cantBarras -= 1000

                ini = candles_tpm[0][0]

                if(primera):
                    candles = candles_tpm.copy() + candles
                    primera = False
                else:
                    candles = candles_tpm[: -1].copy() + candles

            else:
                
                if(cantBarras == 0):
                    
                    pbar(100)

                    del pbar 

                    return {'status': ['ok', ''], 'out': candles}
                    
                sleep(_espera)
                            
                try:
            
                    candles_tpm = self.client.get_klines(
                            symbol = self.simbolo, 
                            interval = _interval, 
                            limit = cantBarras,
                            endTime = _ini
                        )
                    
                except BinanceAPIException as error:
                    del pbar 

                    return {'status': ['error', str(error)], 'out': candles}

                if(primera):
                    candles = candles_tpm.copy() + candles
                    primera = False
                else:
                    candles = candles_tpm[: -1].copy() + candles

                pbar(100)
                
                del pbar

                return {'status': ['ok', ''], 'out': candles}
             
            pbar(len(candles) / totalBarras * 100)


####    Metodo generarDF(
            self, 
            _interval:str,
            _ini:int, 
            _cantBarras:str,
            _espera:float
        ) -> dict[list[str, str], DataFrame]:
        """
        Genera dataFrame de velas para un timeframe (_interval). Donde:
    
         * _interval: TimeFrame a consultar, ver constantes KLINE_INTERVAL
            https://python-binance.readthedocs.io/en/latest/constants.html

         * _ini: Fecha mas cercana mayor a la fecha de la barra ubicada en el
                 indice 0,

         * _cantBarras: Cantidad de barras hacia atras que va a descargar

         * _espera: segundos que espera el programa para hacer una petición
        """

        dict_velas = self.scrapearTickers(
                _interval = _interval,
                _ini = _ini,
                cantBarras = _cantBarras,
                _espera = _espera
            )

        dict_columnas = {
            'Open time':'int64',
            'Open':'float',
            'High':'float',
            'Low':'float',
            'Close':'float',
            'Volume':'float',
            'Close time':'int64',
            'Quote asset volume':'float',
            'Number of trades':'int64',
            'Taker buy base asset volume':'float',
            'Taker buy quote asset volume':'float',
            'Ignore':'float'
            }

        df = DataFrame(dict_velas['out'], columns = dict_columnas.keys())

        df = df.astype(dict_columnas)

        return {'status': dict_velas['status'], 'out': df}
