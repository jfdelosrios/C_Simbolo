# -*- coding: utf-8 -*-
"""C_Simbolo.py"""

from binance.client import Client
from varios import floor2


class C_Simbolo:
    """
    Clase que representa un simbolo determinado. En la practica es como si 
    tuviera dos constructores:

    __init__(monedaBase:str, monedaCotizada:str) -> None

    __init__(simbolo:str) -> None

    Si el simbolo no existe dispara la excepcion binance.exceptions.BinanceAPIException

    Cualquier informacion que requiriese buscar en:
    
    https://python-binance.readthedocs.io/en/latest/

    https://binance-docs.github.io/apidocs/spot/en/#
    """

    def __init__(self, **kwargs) -> None:
        """."""

        try:
            
            if(set(kwargs.keys()) == set(['_symbol'])):

                self.simbolo = kwargs['_symbol'].upper()
                self.monedaBase = 'base'
                self.monedaCotizada = 'cotizada'


            if(set(kwargs.keys()) == set(['monedaBase', 'monedaCotizada'])):
        
                self.monedaBase = kwargs['monedaBase'].upper()
                self.monedaCotizada = kwargs['monedaCotizada'].upper()
                self.simbolo = kwargs['monedaBase'].upper() + kwargs['monedaCotizada'].upper()

        except KeyError:
            
            return
        
        self.client = Client()
        self.simbol_info = self.client.get_symbol_info(self.simbolo)

        #esto es para ver si el simbolo existe
        self.Refresh()


    def Simbol_info(self) -> dict:
        """ """
        return self.simbol_info


    def get_monedaBase(self) -> str:
        """ """

        return self.monedaBase


    def get_monedaCotizada(self) -> str:
        """ """

        return self.monedaCotizada


    def Name(self) -> str:
        """ """

        return self.simbolo


    def Refresh(self) -> None:
        """ """

        self.order_book = self.client.get_order_book(symbol = self.simbolo)
    
    def Order_book(self) -> dict:
        """Nivel 2"""

        self.Refresh()
        return self.order_book

    
    def Ask(self) -> float:
        """ """

        self.Refresh()
        return float(self.order_book['asks'][0][0])

    
    def Bid(self) -> float:
        """ """

        self.Refresh()
        return float(self.order_book['bids'][0][0])


    def avg_price(self) -> dict:
        """ """
        
        return self.client.get_avg_price(symbol = self.simbolo)


    def point(self) -> float:
        """valor del punto."""

        for i in self.Simbol_info()['filters']:

            if(i['filterType'] != 'PRICE_FILTER'):
                continue

            return float(i['tickSize'])

        return -1


    def digits(self) -> int:
        """cantidad de digitos decimales del precio."""

        for i in self.Simbol_info()['filters']:

            if(i['filterType'] != 'PRICE_FILTER'):
                continue

            string = i['tickSize']
            string = string.rstrip('0')
            return (len(string) - string.find('.') - 1)

        return -1


    def formatear_precio(self, precio:float) -> dict:
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

        
    def lote_stepSize(self) -> int:
        """cantidad de digitos decimales del lote."""

        for i in self.Simbol_info()['filters']:

            if(i['filterType'] != 'LOT_SIZE'):
                continue

            string = i['stepSize']
            string = string.rstrip('0')
            return (len(string) - string.find('.') - 1)

        return -1


    def formatear_cant_monedas(self, cant:float) -> dict:
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