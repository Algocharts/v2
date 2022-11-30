import time
import json
import requests
import base64
import mysql.connector
import pickle
from mysql.connector import pooling
from multiprocessing.pool import ThreadPool as multihilo

#Definitions
debug_mode = 2 # 0 = no debug, 1 = no multithread, 2 = no multithread+print things
direccion = 1 # 1 = FORWARD(index new trades), 0 = BACKWARDS(index historical, stops automatically)
asa_memoria = {}
fichapool_memoria = {}
verificados = []
lista_pools = []

try:
	with open("/var/lib/algorand/algod.token", "r") as fichero:
		algod_token = fichero.read()
except:
	algod_token = None


algocharts = mysql.connector.pooling.MySQLConnectionPool(pool_name = "algocharts",pool_size = 24,host="localhost",user="pablo",	password="algocharts",database="algocharts")

class conexion(object):
	def __init__(self):
		inicio=""

def pool_datos(pool):
	datos_pool = session.get(f'https://mainnet-idx.algonode.cloud/v2/accounts/{pool}')
	parse_pool = json.loads(datos_pool.text)
	fichapool = parse_pool['account']['created-assets'][0]['index']
	if fichapool not in fichapool_memoria: fichapool_memoria[fichapool] = asa_datos(fichapool)
	if len(parse_pool['account']['assets']) == 2:
		if parse_pool['account']['assets'][0]['asset-id'] == fichapool: asa_1 = parse_pool['account']['assets'][1]['asset-id'] 
		else: asa_1 = parse_pool['account']['assets'][0]['asset-id']
		asa_2 = 0
		if asa_1 not in asa_memoria: asa_memoria[asa_1] = asa_datos(asa_1)
	elif len(parse_pool['account']['assets']) == 3:
		if parse_pool['account']['assets'][0]['asset-id'] == fichapool:
			asa_1 = parse_pool['account']['assets'][1]['asset-id']
			asa_2 = parse_pool['account']['assets'][2]['asset-id']
		elif parse_pool['account']['assets'][1]['asset-id'] == fichapool:
			asa_1 = parse_pool['account']['assets'][0]['asset-id']
			asa_2 = parse_pool['account']['assets'][2]['asset-id']
		elif parse_pool['account']['assets'][2]['asset-id'] == fichapool:
			asa_1 = parse_pool['account']['assets'][0]['asset-id']
			asa_2 = parse_pool['account']['assets'][1]['asset-id']
		if asa_1 not in asa_memoria: asa_memoria[asa_1] = asa_datos(asa_1)
		if asa_2 not in asa_memoria: asa_memoria[asa_2] = asa_datos(asa_2)
	return asa_1, asa_2, fichapool

def pool_lookup(pool):
	url = f"https://mainnet-idx.algonode.cloud/v2/accounts/{pool}/transactions?min-round={i}&max-round={i}"
	urltx = session.get(url=url, headers={'Connection': 'close'})
	url_parse = json.loads(urltx.text)
	return url_parse

def volumen_tm(grupo, txs, pool, asa1): #volumen TINYMAN1.1 (30m)
	tvol1 = 0; tvol2 = 0
	for transacciones in txs:
		if transacciones['group'] == grupo:
			if 'payment-transaction' in transacciones.keys():
				if transacciones['payment-transaction']['amount'] > 2000: 
					vol2 = transacciones['payment-transaction']['amount']

			if 'asset-transfer-transaction' in transacciones.keys():
					if transacciones['asset-transfer-transaction']['asset-id'] == asa1:
						vol1 = transacciones['asset-transfer-transaction']['amount']
					else: vol2 = transacciones['asset-transfer-transaction']['amount']
	tvol1 = tvol1 + vol1
	tvol2 = tvol2 + vol2
	if debug_mode == 2:
		print(pool)
		print(grupo)
		print(tvol1)
		print(tvol2)
	return tvol1, tvol2


def volumen_af_pf(grupo, txs, pool, asa1): #volumen PACTFI y ALGOFI y HUMBLE
	tvol1 = 0; tvol2 = 0
	print(grupo)
	for transacciones in txs:
		if transacciones['group'] == grupo:
			if 'payment-transaction' in transacciones.keys():
				if transacciones['payment-transaction']['amount'] > 2000: 
					vol2 = transacciones['payment-transaction']['amount']

			if 'asset-transfer-transaction' in transacciones.keys():
					if transacciones['asset-transfer-transaction']['asset-id'] == asa1:
						vol1 = transacciones['asset-transfer-transaction']['amount']
					else: vol2 = transacciones['asset-transfer-transaction']['amount']

			if 'inner-txns' in transacciones.keys():
				if 'payment-transaction' in transacciones['inner-txns'][0]:
					vol2 = transacciones['inner-txns'][0]['payment-transaction']['amount']
				elif 'asset-transfer-transaction' in transacciones['inner-txns'][0]:
					if transacciones['inner-txns'][0]['asset-transfer-transaction']['asset-id'] == asa1:
						vol1 = transacciones['inner-txns'][0]['asset-transfer-transaction']['amount']
					else: vol2 = transacciones['inner-txns'][0]['asset-transfer-transaction']['amount']
	tvol1 = tvol1 + vol1
	tvol2 = tvol2 + vol2
	if debug_mode == 2:
		print(pool)
		print(grupo)
		print(vol1)
		print(vol2)
	return tvol1, tvol2


def volumen_af(grupo):
	return None
def volumen_hb(grupo):
	return None
#markets: 1 = tinyman11, 2 = algofi, 3 = pactfi, 4 = humble2
def precio(pool):
	contador = 0
	asas = pool_datos(pool)
	if debug_mode == 2: print(asas)
	url_parse = pool_lookup(pool)
	for transacciones in url_parse['transactions']:
		if transacciones['tx-type'] == 'appl':
				if 'c3dhcA==' in transacciones['application-transaction']['application-args']:
					volumen = volumen_tm(transacciones['group'], url_parse['transactions'], pool, asas[0])
					if debug_mode == 2: print("TINYMAN TX")
					mercado = "1"; contador = contador + 1
					for x in transacciones['local-state-delta']:
						for w in x['delta']:
							if w['key'] == "czE=": liq1 = w['value']['uint']
							if w['key'] == "czI=": liq2 = w['value']['uint']
				elif 'c2Vm' in transacciones['application-transaction']['application-args'] and len(transacciones['application-transaction']['foreign-apps']) == 0:
				#https://docs.algofi.org/algofi-nanoswap/mainnet-contracts
					volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
					if debug_mode == 2: print("ALGOFI TX")
					mercado = "2"
					contador = contador + 1
					for x in transacciones['global-state-delta']:
						if x['key'] == "YjI=": liq1 =  x['value']['uint']
						if x['key'] == "YjE=": liq2 = x['value']['uint']
				elif 'U1dBUA==' in transacciones['application-transaction']['application-args']:
					volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
					if debug_mode == 2: print("PACTFI TX")
					mercado = "3"
					contador = contador + 1
					for x in transacciones['global-state-delta']:
						if x['key'] == "Qg==": liq1 =  x['value']['uint']
						if x['key'] == "QQ==": liq2 = x['value']['uint']
				elif all(item in transacciones['application-transaction']['application-args'] for item in ['AA==', 'Aw==', 'AAAAAAAAAAA=']):
					volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
					if debug_mode == 2: print("HUMBLE TX")
					mercado = "4"
					contador = contador + 1
					liq2 = int.from_bytes(base64.b64decode(transacciones['global-state-delta'][1]['value']['bytes'])[-38:-30], 'big')
					liq1 = int.from_bytes(base64.b64decode(transacciones['global-state-delta'][1]['value']['bytes'])[-30:-22], 'big')

	if 'liq1' and 'liq2' not in locals(): return
	tabla = (str(asas[0]) + "_" + str(asas[1]))
	conexion = algocharts.get_connection()
	cursor = conexion.cursor()
	sql = "CREATE TABLE IF NOT EXISTS %s (timestamp bigint unsigned PRIMARY KEY, liqa1 bigint unsigned NOT NULL, liqa2 bigint unsigned NOT NULL, precio DECIMAL(24,12) NOT NULL, vol1 bigint unsigned not null, vol2 bigint unsigned not null, tx int, market tinyint unsigned not null)" % tabla
	cursor.execute(sql)
	conexion.commit()
	sql = f"INSERT IGNORE INTO {tabla} (timestamp, liqa1, liqa2, precio, vol1, vol2, tx, market) VALUES ( {int(transacciones['round-time'])}, {liq1}, {liq2}, {(liq2/liq1)*ajuste_decimales(asas[0],asas[1])}, {volumen[0]}, {volumen[1]}, {contador}, {mercado} )"
	cursor.execute(sql)
	conexion.commit()
	sql = "INSERT INTO pools (asa1, asa2, pool, market, lptoken, liqa1, liqa2) VALUES ( %s, %s, %s, %s, %s, %s, %s ) ON DUPLICATE KEY UPDATE liqa1 = %s, liqa2 = %s"
	valores = ( int(asas[0]), int(asas[1]), pool, mercado, int(asas[2]), liq1, liq2, liq1, liq2)
	cursor.execute(sql, valores)
	conexion.commit()
	cursor.close()
	conexion.close()


def ajuste_decimales(asa1, asa2):
	return pow(10,(asa_memoria.get(asa1)['decimals']-asa_memoria.get(asa2)['decimals']))

def obtener_verificados():
	verificados.clear()
	obtener_verificados = session.get(f'https://mobile-api.algorand.com/api/verified-assets/')
	parsear_verificados = json.loads(obtener_verificados.text)
	for j in parsear_verificados['results']:
		verificados.append(j['asset_id'])

def verificado(asa):
	if asa not in verificados:
		return "0"
	if asa in verificados:
		return "1"

def obtener_circulating(reserve, asa, total):
	obtener_circulating = session.get(f'https://mainnet-idx.algonode.cloud/v2/accounts/{reserve}')
	parsear_circulating = json.loads(obtener_circulating.text)
	for x in parsear_circulating['account']['assets']:
		if x['asset-id'] == asa:
			reservas = x['asset-id']
			break
	return total-reservas

def asa_datos(asa):
	obtener_asa = session.get(f'https://mainnet-idx.algonode.cloud/v2/assets/{asa}')
	parsear_asa = json.loads(obtener_asa.text)
	if parsear_asa['asset']['params']['reserve'] == "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAY5HFKQ":
		circulating = parsear_asa['asset']['params']['total']
	else:
		circulating = obtener_circulating(parsear_asa['asset']['params']['reserve'], asa, parsear_asa['asset']['params']['total'])
	try:
		url = parsear_asa['asset']['params']['url']
	except KeyError:
		url = None
	if 'name' in parsear_asa['asset']['params']:
		nombre = parsear_asa['asset']['params']['name']
	else:
		nombre =  str(base64.b64decode(parsear_asa['asset']['params']['name-b64'])).split("\\")[0].split("'")[1]
	if 'unit-name' in parsear_asa['asset']['params']:
		unidad = parsear_asa['asset']['params']['unit-name']
	else:
		unidad = str(base64.b64decode(parsear_asa['asset']['params']['unit-name-b64'])).split("\\")[0].split("'")[1]

	diccionario_asa = {
	'id': parsear_asa['asset']['index'],
	'name': nombre,
	'unit_name': unidad,
	'total_amount': parsear_asa['asset']['params']['total'],
	'url': url,
	'decimals': parsear_asa['asset']['params']['decimals'],
	'is_verified': f"{verificado(asa)}",
	'circulating_supply': circulating,
	'creator': parsear_asa['asset']['params']['creator'],
	'reserve': parsear_asa['asset']['params']['reserve'] }
	asa_memoria[0] = {'id': 0, 'name': "Algorand", 'unit_name': "ALGO", 'total_amount': 10000000000000000, 'url': "https://www.algorand.org", 'decimals': 6, 'is_verified': 1,
	'circulating_supply': 7115148926730000,
	'creator': "",
	'reserve': ""
	}
	return diccionario_asa


session = requests.Session()
lsession = requests.Session()
#AQUI EMPIEZA/START HERE
sincronizado = False
obtener_verificados()
try:
	with open("last-block", "r") as fichero:
		i = int(fichero.read())
except:
	i = 25200000
	pass

while True:
	if i % 1000 == 0:
		asa_memoria.clear()
		if sincronizado == True: obtener_verificados()


	obtener_bloque = session.get(f'https://mainnet-idx.algonode.cloud/v2/blocks/{i}')
	parsear_bloque = json.loads(obtener_bloque.text)
	if len(parsear_bloque) < 2:
		time.sleep(2)
		sincronizado = True
		print(f'Still not {i}! ')
		continue
	else:
		bloque_dicc = parsear_bloque['transactions']
		for x in bloque_dicc:
			if x['tx-type'] == 'appl':
				try:
					if 'c3dhcA==' in x['application-transaction']['application-args'] and x['local-state-delta'][0]['address'] not in lista_pools:
						lista_pools.append(x['local-state-delta'][0]['address'])
					elif 'c2Vm' in x['application-transaction']['application-args'] and x['inner-txns'][0]['sender'] not in lista_pools:
						lista_pools.append(x['inner-txns'][0]['sender'])
					elif 'U1dBUA==' in x['application-transaction']['application-args'] and x['inner-txns'][0]['sender'] not in lista_pools:
						lista_pools.append(x['inner-txns'][0]['sender'])
					elif all(y in x['application-transaction']['application-args'] for y in ['AA==', 'Aw==', 'AAAAAAAAAAA=']):
						if len(x['application-transaction']['application-args'][3]) > 24:
							lista_pools.append(x['inner-txns'][0]['sender'])
				except Exception as e:
					print(e)


		paralelo = multihilo(16)
		for y in lista_pools:
			if debug_mode == 0: paralelo.apply_async(precio, (y,))
			else: precio(y)

		if debug_mode == 0:
			paralelo.close()
			paralelo.join()


		lista_pools.clear()
		if direccion == 1:
			i = i + 1
		else:
			i = i - 1
			if i < 16500000: break #pre-markets block

		with open("last-block", "w") as fichero:
			fichero.write(str(i))
