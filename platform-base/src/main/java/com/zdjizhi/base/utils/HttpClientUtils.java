package com.zdjizhi.base.utils;


import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;


public class HttpClientUtils {

    private static Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);
    public static final String ERROR_MESSAGE = "-1";

    private PoolingHttpClientConnectionManager getSslClientManager() {
        // 创建ConnectionManager，添加Connection配置信息
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        // 设置最大连接数
        connManager.setMaxTotal(Configs.get(CommonConfig.HTTP_POOL_MAX_CONNECTION));
        // 设置每个连接的路由数
        connManager.setDefaultMaxPerRoute(Configs.get(CommonConfig.HTTP_POOL_MAX_PER_ROUTE));
        return connManager;
    }

    /**
     * 获取Http客户端连接对象
     * @return Http客户端连接对象
     */
    private  CloseableHttpClient getHttpClient() {
        // 创建Http请求配置参数
        RequestConfig requestConfig = RequestConfig.custom()
                // 获取连接超时时间
                .setConnectionRequestTimeout(Configs.get(CommonConfig.HTTP_POOL_REQUEST_TIMEOUT))
                // 请求超时时间
                .setConnectTimeout(Configs.get(CommonConfig.HTTP_POOL_CONNECT_TIMEOUT))
                // 响应超时时间
                .setSocketTimeout(Configs.get(CommonConfig.HTTP_POOL_RESPONSE_TIMEOUT))
                .build();

        // 创建httpClient
        return HttpClients.custom()
                // 把请求相关的超时信息设置到连接客户端
                .setDefaultRequestConfig(requestConfig)
                // 配置连接池管理对象
                .setConnectionManager(getSslClientManager())
                .build();
    }

    /**
     * GET请求
     *
     * @param uri 请求地
     * @return message
     */
    public  String httpGet(URI uri, Header... headers) {
        String msg = ERROR_MESSAGE;

        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        CloseableHttpResponse response = null;

        try {
            logger.info("http get uri {}",uri);
            // 创建GET请求对象
            HttpGet httpGet = new HttpGet(uri);

            for (Header h : headers) {
                httpGet.addHeader(h);
                logger.info("request header : {}",h);
            }
            // 执行请求
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");

            if (statusCode != HttpStatus.SC_OK) {
                logger.error("Http get content is :{}" , msg);
            }

        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
            logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
            logger.error("IO错误: {}",e.getMessage());
        }  finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }

        return msg;
    }
    /**
     * POST 请求
     * @param uri uri参数
     * @param requestBody 请求体
     * @return post请求返回结果
     */
    public  String httpPost(URI uri, String requestBody, Header... headers) {
    	String msg = ERROR_MESSAGE;
    	// 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        
        // 创建POST请求对象
        CloseableHttpResponse response = null;
        try {

            logger.info("http post uri:{}， http post body:{}", uri, requestBody);
            
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
            for (Header h : headers) {
                httpPost.addHeader(h);
                logger.info("request header : {}",h);
            }

            if(StringUtils.isNotBlank(requestBody)) {
                byte[] bytes = requestBody.getBytes(StandardCharsets.UTF_8);
                httpPost.setEntity(new ByteArrayEntity(bytes));
            }
            
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");
            
            if (statusCode != HttpStatus.SC_OK) {
                logger.error("Http post content is :{}" , msg);
            }
        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
        	logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
        	logger.error("IO错误: {}", e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                	logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }
        return msg;
    }

    /**
     * 拼装url
     * url ,参数map
     */
    public static void setUrlWithParams(URIBuilder uriBuilder,String path, Map<String, Object> params) {
        try {
            uriBuilder.setPath(path);
            if (params != null && !params.isEmpty()){
                for (Map.Entry<String, Object> kv : params.entrySet()) {
                    uriBuilder.setParameter(kv.getKey(),kv.getValue().toString());
                }
            }
        } catch (Exception e) {
            logger.error("拼接url出错,uri : {}, path : {},参数: {}", uriBuilder, path, params);
        }
    }



    public byte[] httpGetInputStream(String url, Header... headers) {
        byte[] result = new byte[0];
        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        // 创建GET请求对象
        HttpGet httpGet = new HttpGet(url);
        for (Header h : headers) {
            httpGet.addHeader(h);
        }
        CloseableHttpResponse response = null;

        try {
            // 执行请求
            response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(response.toString());
            }
            // 获取响应实体
            result = IOUtils.toByteArray(response.getEntity().getContent());
            // 获取响应信息
            EntityUtils.consume(response.getEntity());
        } catch (IOException e) {
            logger.error("current file: {},IO error:{}", url, e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("Release Connection error:{}", e.getMessage());
                }
            }
        }
        return result;
    }

}