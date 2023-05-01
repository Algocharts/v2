import time
import json
import requests
import base64
import mysql.connector
from mysql.connector import pooling
from multiprocessing.pool import ThreadPool as multihilo

####################
##Opciones/OPTIONS##
####################
opt_debug = True 			#debug, print whats going on
opt_protocol_debug = False		#debug this design itself
opt_print_current_block = True	 	#more debug
opt_multi = False 			#multithread on/off
opt_reverse = False 			#go backwards
opt_local_node = False			#use local node


#######################
##Funciones/FUNCTIONS##
#######################
class conexion(object):
	def __init__(self):
		inicio=""

def pool_datos(pool):
	if opt_local_node == True:
		datos_pool = lsession.get(f'http://127.0.0.1:8080/v2/accounts/{pool}')
	else:
		datos_pool = session.get(f'https://mainnet-idx.algonode.cloud/v2/accounts/{pool}')
	parse_pool = json.loads(datos_pool.text)
	try:
		fichapool = parse_pool['account']['created-assets'][0]['index']
	except KeyError:
		try:
			for z in parse_pool['account']['apps-local-state'][0]['key-value']:
				if z['key'] == "cG9vbF90b2tlbl9hc3NldF9pZA==": #tinyman2 hack
					fichapool =  z['value']['uint']
		except KeyError:
			return None
	if len(parse_pool['account']['assets']) < 2 or len(parse_pool['account']['assets']) > 3: return None
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
	if opt_local_node == True and opt_reverse == False and sincronizado == True:
		url = f"http://127.0.0.1:8080/v2/accounts/{pool}/transactions?min-round={i}&max-round={i}"
		urltx = lsession.get(url=url, headers={'Connection': 'close'})
	else:
		url = f"https://mainnet-idx.algonode.cloud/v2/accounts/{pool}/transactions?min-round={i}&max-round={i}"
		urltx = session.get(url=url, headers={'Connection': 'close'})
	url_parse = json.loads(urltx.text)
	return url_parse

def volumen_tm(grupo, txs, pool, asa1): #volumen TINYMAN 1 y 1.1
	if opt_debug == True: print("TX POOL: " + pool + " TX GROUP " + grupo)
	tvol1 = 0; tvol2 = 0; feetx = False #just because some swaps are 2000 malgo
	for transacciones in txs:
		if transacciones['group'] == grupo:
			if 'payment-transaction' in transacciones.keys():
				if feetx == True and transacciones['payment-transaction']['amount'] == 2000:
					vol2 = 2000
				if transacciones['payment-transaction']['amount'] == 2000:
					feetx = True
				if transacciones['payment-transaction']['amount'] != 2000:
					vol2 = transacciones['payment-transaction']['amount']


			if 'asset-transfer-transaction' in transacciones.keys():
					if transacciones['asset-transfer-transaction']['asset-id'] == asa1:
						vol1 = transacciones['asset-transfer-transaction']['amount']
					else: vol2 = transacciones['asset-transfer-transaction']['amount']
	if opt_protocol_debug == False:
		if 'vol1' and 'vol2' not in locals():
			return None
	tvol1 = tvol1 + vol1; tvol2 = tvol2 + vol2
	if opt_debug == True: print("volume1: " + str(tvol1) + " volume2: " + str(tvol2))
	return tvol1, tvol2

def volumen_af_pf(grupo, txs, pool, asa1): #volumen PACTFI y ALGOFI y HUMBLE
	if opt_debug == True: print("TX POOL: " + pool + " TX GROUP " + grupo)
	tvol1 = 0; tvol2 = 0
	for transacciones in txs:
		if 'group' in transacciones.keys():
			if transacciones['group'] == grupo:
				if 'payment-transaction' in transacciones.keys():
					if transacciones['payment-transaction']['amount'] > 2000:
						vol2 = transacciones['payment-transaction']['amount']

				if 'asset-transfer-transaction' in transacciones.keys():
					if transacciones['asset-transfer-transaction']['asset-id'] == asa1:
						vol1 = transacciones['asset-transfer-transaction']['amount']
					else: vol2 = transacciones['asset-transfer-transaction']['amount']

				try:
					pos_array = 0
					if 'inner-txns' in transacciones.keys():
						if 'payment-transaction' in transacciones['inner-txns'][pos_array]:
							vol2 = transacciones['inner-txns'][pos_array]['payment-transaction']['amount']
						elif 'asset-transfer-transaction' in transacciones['inner-txns'][pos_array]:
							if transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['asset-id'] == asa1:
								vol1 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']
							else: vol2 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']
				except IndexError:
					pos_array = 4
					if 'inner-txns' in transacciones.keys():
						if 'payment-transaction' in transacciones['inner-txns'][pos_array]:
							vol2 = transacciones['inner-txns'][pos_array]['payment-transaction']['amount']
						elif 'asset-transfer-transaction' in transacciones['inner-txns'][pos_array]:
							if transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['asset-id'] == asa1:
								vol1 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']
							else: vol2 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']

		else:
			txs_inner = transacciones['inner-txns']
			for transacciones_int in txs_inner:
				if 'group' in transacciones_int.keys():
					if 'payment-transaction' in transacciones.keys():
						if transacciones['payment-transaction']['amount'] > 2000:
							vol2 = transacciones['payment-transaction']['amount']

					if 'asset-transfer-transaction' in transacciones.keys():
						if transacciones['asset-transfer-transaction']['asset-id'] == asa1:
							vol1 = transacciones['asset-transfer-transaction']['amount']
						else: vol2 = transacciones['asset-transfer-transaction']['amount']

					try:
						pos_array = 0
						if 'inner-txns' in transacciones.keys():
							if 'payment-transaction' in transacciones['inner-txns'][pos_array]:
								vol2 = transacciones['inner-txns'][pos_array]['payment-transaction']['amount']
							elif 'asset-transfer-transaction' in transacciones['inner-txns'][pos_array]:
								if transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['asset-id'] == asa1:
									vol1 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']
								else: vol2 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']
					except IndexError:
						pos_array = 4
						if 'inner-txns' in transacciones.keys():
							if 'payment-transaction' in transacciones['inner-txns'][pos_array]:
								vol2 = transacciones['inner-txns'][pos_array]['payment-transaction']['amount']
							elif 'asset-transfer-transaction' in transacciones['inner-txns'][pos_array]:
								if transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['asset-id'] == asa1:
									vol1 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']
								else: vol2 = transacciones['inner-txns'][pos_array]['asset-transfer-transaction']['amount']



	if opt_protocol_debug == False:
		if 'vol1' and 'vol2' not in locals():
			return None
	tvol1 = tvol1 + vol1; tvol2 = tvol2 + vol2
	if opt_debug == True: print("volume1: " + str(tvol1) + " volume2: " + str(tvol2))
	return tvol1, tvol2

def volumen_tm2(grupo, txs, pool, asa1): #volumen TM2
	if opt_debug == True: print("TX POOL: " + pool + " TX GROUP " + grupo)
	tvol1 = 0; tvol2 = 0
	for transacciones in txs:
		if 'group' in transacciones.keys():
			if transacciones['group'] == grupo:
				if 'payment-transaction' in transacciones.keys():
					if transacciones['payment-transaction']['amount'] > 2000:
						vol2 = transacciones['payment-transaction']['amount']

				if 'asset-transfer-transaction' in transacciones.keys():
						if transacciones['asset-transfer-transaction']['asset-id'] == asa1:
							vol1 = transacciones['asset-transfer-transaction']['amount']
						else: vol2 = transacciones['asset-transfer-transaction']['amount']

				if 'inner-txns' in transacciones.keys():
						for tx in transacciones['inner-txns']:
							if 'payment-transaction' in tx:
								vol2 = tx['payment-transaction']['amount']
							elif 'asset-transfer-transaction' in tx:
								if tx['asset-transfer-transaction']['asset-id'] == asa1:
									vol1 = tx['asset-transfer-transaction']['amount']
								else: vol2 = tx['asset-transfer-transaction']['amount']
		else:
			txs_inner = transacciones['inner-txns']
			for transacciones_int in txs_inner:
				if 'group' in transacciones_int.keys():
					if transacciones_int['group'] == grupo:
						if 'payment-transaction' in transacciones_int.keys():
							if transacciones_int['payment-transaction']['amount'] > 2000:
								vol2 = transacciones_int['payment-transaction']['amount']

						if 'asset-transfer-transaction' in transacciones_int.keys():
								if transacciones_int['asset-transfer-transaction']['asset-id'] == asa1:
									vol1 = transacciones_int['asset-transfer-transaction']['amount']
								else: vol2 = transacciones_int['asset-transfer-transaction']['amount']


	if opt_protocol_debug == False:
		if 'vol1' and 'vol2' not in locals():
			return None
	tvol1 = tvol1 + vol1; tvol2 = tvol2 + vol2
	if opt_debug == True: print("volume1: " + str(tvol1) + " volume2: " + str(tvol2))
	return tvol1, tvol2



#mercados(markets): 1 = tinyman1.0/tinyman1.1, 2 = algofi, 3 = pactfi, 4 = humble2, 5 = tinyman2
def precio(pool):
	contador = 0
	asas = pool_datos(pool)
	if asas == None: return
	nanoswap = False
	url_parse = pool_lookup(pool)
	for transacciones in url_parse['transactions']:
		if transacciones['tx-type'] == 'appl':
				if 'c3dhcA==' in transacciones['application-transaction']['application-args']:
					if transacciones['application-transaction']['application-id'] == 1002541853:
						if opt_debug == True: print("TINYMAN 2 TX " + str(asas))
						volumen = volumen_tm2(transacciones['group'], url_parse['transactions'], pool, asas[0])
						mercado = 5; contador = contador + 1
						for x in transacciones['local-state-delta']:
							for w in x['delta']:
								if w['key'] == "YXNzZXRfMV9yZXNlcnZlcw==": liq1 = w['value']['uint']
								if w['key'] == "YXNzZXRfMl9yZXNlcnZlcw==": liq2 = w['value']['uint']
					elif transacciones['application-transaction']['application-id'] == 552635992 or transacciones['application-transaction']['application-id'] == 350338509:
						if opt_debug == True: print("TINYMAN 1 TX " + str(asas))
						volumen = volumen_tm(transacciones['group'], url_parse['transactions'], pool, asas[0])
						mercado = 1; contador = contador + 1
						for x in transacciones['local-state-delta']:
							for w in x['delta']:
								if w['key'] == "czE=": liq1 = w['value']['uint']
								if w['key'] == "czI=": liq2 = w['value']['uint']
				elif 'c2Vm' in transacciones['application-transaction']['application-args']: 
					if transacciones['application-transaction']['foreign-apps'] != 658336870: #https://docs.algofi.org/algofi-nanoswap/mainnet-contracts
						if opt_debug == True: print("ALGOFI TX " + str(asas))
						volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
						mercado = 2
						contador = contador + 1
						for x in transacciones['global-state-delta']:
							if x['key'] == "YjI=": liq1 =  x['value']['uint']
							if x['key'] == "YjE=": liq2 = x['value']['uint']
					elif transacciones['application-transaction']['foreign-apps'] == 658336870:
						nanoswap = True
						if opt_debug == True: print("ALGOFI  NANOSWAP TX " + str(asas))
						volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
						mercado = 2
						contador = contador + 1
						for x in transacciones['global-state-delta']:
							if x['key'] == "YjI=": liq1 =  x['value']['uint']
							if x['key'] == "YjE=": liq2 = x['value']['uint']

				elif 'U1dBUA==' in transacciones['application-transaction']['application-args']:
					if opt_debug == True: print("PACTFI TX " + str(asas))
					volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
					mercado = 3
					contador = contador + 1
					for x in transacciones['global-state-delta']:
						if x['key'] == "Qg==": liq1 =  x['value']['uint']
						if x['key'] == "QQ==": liq2 = x['value']['uint']
				elif all(item in transacciones['application-transaction']['application-args'] for item in ['AA==', 'Aw==', 'AAAAAAAAAAA=']):
					if opt_debug == True: print("HUMBLE TX " + str(asas))
					if int.from_bytes(base64.b64decode(transacciones['application-transaction']['application-args'][3])[:1], 'big') != 4: return #4 trade, 2 liquidity ?
					volumen = volumen_af_pf(transacciones['group'], url_parse['transactions'], pool, asas[0])
					mercado = 4
					contador = contador + 1
					liq2 = int.from_bytes(base64.b64decode(transacciones['global-state-delta'][1]['value']['bytes'])[-38:-30], 'big')
					liq1 = int.from_bytes(base64.b64decode(transacciones['global-state-delta'][1]['value']['bytes'])[-30:-22], 'big')

	if 'liq1' and 'liq2' not in locals(): return
	if nanoswap == True and mercado == 3: 
		mercado = 2
		if opt_debug == True: print("ALGOFI NANOSWAP(R) TX " + str(asas))
	tabla = (str(asas[0]) + "_" + str(asas[1]))
	conexion = algocharts.get_connection()
	cursor = conexion.cursor()
	sql = "CREATE TABLE IF NOT EXISTS %s (timestamp TIMESTAMP PRIMARY KEY, liqa1 bigint unsigned NOT NULL, liqa2 bigint unsigned NOT NULL, precio DECIMAL(24,12) NOT NULL, vol1 bigint unsigned not null, vol2 bigint unsigned not null, tx int, market tinyint unsigned not null)" % tabla
	cursor.execute(sql)
	#conexion.commit()
	sql = f"INSERT IGNORE INTO {tabla} (timestamp, liqa1, liqa2, precio, vol1, vol2, tx, market) VALUES ( FROM_UNIXTIME( {int(transacciones['round-time'])} ), {liq1}, {liq2}, {(liq2/liq1)*ajuste_decimales(asas[0],asas[1])}, {volumen[0]}, {volumen[1]}, {contador}, {mercado} )"
	cursor.execute(sql)
	#conexion.commit()
	if opt_reverse == False:
		sql = "INSERT INTO pools (asa1, asa2, pool, market, lptoken, liqa1, liqa2) VALUES ( %s, %s, %s, %s, %s, %s, %s ) ON DUPLICATE KEY UPDATE liqa1 = %s, liqa2 = %s"
	else:
		sql = "INSERT IGNORE INTO pools (asa1, asa2, pool, market, lptoken, liqa1, liqa2) VALUES ( %s, %s, %s, %s, %s, %s, %s )"
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
	if opt_debug == True:
		print("Obtener circulating: reserve address: " + reserve + " asa: " + str(asa))
	if opt_local_node == True:
		obtener_circulating = lsession.get(f'http://127.0.0.1:8080/v2/accounts/{reserve}')
		parsear_circulating = json.loads(obtener_circulating.text); y = parsear_circulating['account']['assets']
	else:
		obtener_circulating = session.get(f'https://mainnet-idx.algonode.cloud/v2/accounts/{reserve}/assets')
		parsear_circulating = json.loads(obtener_circulating.text); y = parsear_circulating['assets']

	try:
		reservas = 0
		for x in y:
			if x['asset-id'] == asa:
				reservas = x['asset-id']
				break
		return total-reservas
	except KeyError:
		return total

def asa_datos(asa):
	if opt_local_node == True:
		obtener_asa = session.get(f'http://127.0.0.1:8080/v2/assets/{asa}')
	else:
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

#######################################
##Comienzo del programa/PROGRAM START##
#######################################
sincronizado = False
asa_memoria = {}
fichapool_memoria = {}
verificados = []
lista_pools = []


try:
	with open("/var/lib/algorand/algod.token", "r") as f:
		algod_token = f.read()
except:
	algod_token = None

algocharts = mysql.connector.pooling.MySQLConnectionPool(pool_name = "algocharts",pool_size = 24,host="localhost",user="pablo", password="algocharts",database="algocharts")

session = requests.Session()
lsession = requests.Session()

lsession.headers.update({'X-Algo-API-Token':algod_token})

obtener_verificados()

if opt_reverse == False: fichero = "last-block"
else: fichero = "last-block-reverse"

if opt_local_node == True:
	obtener_cblock = lsession.get('http://127.0.0.1:8080/v2/status/')
else:
	obtener_cblock = session.get('https://mainnet-api.algonode.cloud/v2/status')
parsear_cblock = json.loads(obtener_cblock.text); last_round = parsear_cblock['last-round']

try:
	with open(fichero, "r") as f:
		i = int(f.read())

except:
	i = 28718928 # manual block start!!
	pass

if last_round - i < 600:
	sincronizado == True


while True:
	if i % 1000 == 0:
		asa_memoria.clear()
		if sincronizado == True: obtener_verificados()

	if opt_local_node == True and opt_reverse == False and sincronizado == True:
		obtener_bloque = lsession.get(f'http://127.0.0.1:8080/v2/blocks/{i}')
	else:
		obtener_bloque = session.get(f'https://mainnet-idx.algonode.cloud/v2/blocks/{i}')
	parsear_bloque = json.loads(obtener_bloque.text)
	if len(parsear_bloque) < 2:
		time.sleep(2)
		sincronizado = True
		if opt_print_current_block == True: print(f'Still not {i}! ')
		continue
	else:
		if opt_print_current_block == True: print(i)
		bloque_dicc = parsear_bloque['transactions']
		for x in bloque_dicc:
			if x['tx-type'] == 'appl':
				try:
					if 'c3dhcA==' in x['application-transaction']['application-args']:
						if x['application-transaction']['application-id'] == 552635992 or x['application-transaction']['application-id'] == 350338509 and x['local-state-delta'][0]['address'] not in lista_pools:
							lista_pools.append(x['local-state-delta'][0]['address'])
						if x['application-transaction']['application-id'] == 1002541853 and x['inner-txns'][0]['sender'] not in lista_pools:
							lista_pools.append(x['inner-txns'][0]['sender'])
					elif 'c2Vm' in x['application-transaction']['application-args'] and x['inner-txns'][0]['sender'] not in lista_pools:
						lista_pools.append(x['inner-txns'][0]['sender'])
					elif 'U1dBUA==' in x['application-transaction']['application-args'] and x['application-transaction']['application-id'] != 947569965 and x['inner-txns'][0]['sender'] not in lista_pools:
						lista_pools.append(x['inner-txns'][0]['sender'])
					elif all(y in x['application-transaction']['application-args'] for y in ['AA==', 'Aw==', 'AAAAAAAAAAA=']):
						if len(x['application-transaction']['application-args'][3]) > 24:
							lista_pools.append(x['inner-txns'][0]['sender'])
				except Exception as e:
					if opt_debug == True: print(e)


		paralelo = multihilo(16)
		for y in lista_pools:
			if opt_multi == True: paralelo.apply_async(precio, (y,))
			else: precio(y)

		if opt_multi == True:
			paralelo.close()
			paralelo.join()


		lista_pools.clear()
		if opt_reverse == False:
			i = i + 1
		else:
			i = i - 1
			if i < 16500000: break #pre-markets block

		with open(fichero, "w") as f:
			f.write(str(i))
