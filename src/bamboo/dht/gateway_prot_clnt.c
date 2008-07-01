/*
 * Please do not edit this file.
 * It was generated using rpcgen.
 */

#include "gateway_prot.h"

/* Default timeout can be changed using clnt_control() */
static struct timeval TIMEOUT = { 25, 0 };

void *
bamboo_dht_proc_null_2(argp, clnt)
	void *argp;
	CLIENT *clnt;
{
	static char clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_NULL, xdr_void, argp, xdr_void, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return ((void *)&clnt_res);
}

bamboo_stat *
bamboo_dht_proc_put_2(argp, clnt)
	bamboo_put_args *argp;
	CLIENT *clnt;
{
	static bamboo_stat clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_PUT, xdr_bamboo_put_args, argp, xdr_bamboo_stat, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return (&clnt_res);
}

bamboo_get_res *
bamboo_dht_proc_get_2(argp, clnt)
	bamboo_get_args *argp;
	CLIENT *clnt;
{
	static bamboo_get_res clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_GET, xdr_bamboo_get_args, argp, xdr_bamboo_get_res, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return (&clnt_res);
}

void *
bamboo_dht_proc_null_3(argp, clnt)
	void *argp;
	CLIENT *clnt;
{
	static char clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_NULL, xdr_void, argp, xdr_void, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return ((void *)&clnt_res);
}

bamboo_stat *
bamboo_dht_proc_put_3(argp, clnt)
	bamboo_put_arguments *argp;
	CLIENT *clnt;
{
	static bamboo_stat clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_PUT, xdr_bamboo_put_arguments, argp, xdr_bamboo_stat, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return (&clnt_res);
}

bamboo_get_result *
bamboo_dht_proc_get_3(argp, clnt)
	bamboo_get_args *argp;
	CLIENT *clnt;
{
	static bamboo_get_result clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_GET, xdr_bamboo_get_args, argp, xdr_bamboo_get_result, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return (&clnt_res);
}

bamboo_stat *
bamboo_dht_proc_rm_3(argp, clnt)
	bamboo_rm_arguments *argp;
	CLIENT *clnt;
{
	static bamboo_stat clnt_res;

	memset((char *)&clnt_res, 0, sizeof(clnt_res));
	if (clnt_call(clnt, BAMBOO_DHT_PROC_RM, xdr_bamboo_rm_arguments, argp, xdr_bamboo_stat, &clnt_res, TIMEOUT) != RPC_SUCCESS)
		return (NULL);
	return (&clnt_res);
}
