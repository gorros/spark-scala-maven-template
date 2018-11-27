package com.github.gorros.helpers

import requests.Response

import scala.util.{Failure, Success, Try}

object HttpUtils {
    def requestWithRetries[A](url: String, params: Map[String, String] = Map(), tries: Int = 5)
                             (req: (String, Map[String,String]) => Response)(f: String => A): A = {
        Try(req(url, params)) match {
            case Success(r) if r.is2xx =>
                f(r.text)
            case Success(r) if r.is4xx =>
                throw new Exception(s"Failed to extract data from web API!!!\n ${r.text}")
            case Success(r) if r.is5xx =>
                if (tries == 0) {
                    throw new Exception(s"Failed to extract data from web API!!!\n ${r.text}")
                } else {
                    requestWithRetries(url, tries = tries - 1)(req)(f)
                }
            case Failure(e) =>
                if (tries == 0) {
                    throw new Exception("Failed to extract data from web API!!!", e)
                } else {
                    requestWithRetries(url, tries = tries - 1)(req)(f)
                }
        }
    }
}
