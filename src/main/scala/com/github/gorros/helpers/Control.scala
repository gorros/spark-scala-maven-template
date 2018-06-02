package com.github.gorros.helpers

object Control {
    // Loan pattern

    def using[A <: {def close() : Unit}, B](param: A)(f: A => B): B =
        try {
            f(param)
        } finally {
            param.close()
        }

}

