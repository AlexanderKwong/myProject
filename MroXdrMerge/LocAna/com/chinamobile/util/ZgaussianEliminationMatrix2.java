package com.chinamobile.util;

import java.text.DecimalFormat;

/**
 * Copyright © Zhou Xingwei. All Rights Reserved
 * Email: zhouxingwei@139.com
 * Function:
 * Usage:
 */
@NotProguard
public class ZgaussianEliminationMatrix2
{
    public double[] calculate(double[][] a)
    {
        int _rows = a.length;
        int _cols = a[0].length;
        int L = _rows - 1;
        int i, j, l, n, m, k = 0;
        double[] temp1 = new double[_rows];
        /* 第一个do-while是将增广矩阵消成上三角形式 */
        do
        {
            n = 0;
            for (l = k; l < L; l++)
            {
                temp1[n++] = a[l + 1][k] / a[k][k];
            }
            for (m = 0, i = k + 1; i < _rows; i++, m++)
            {
                for (j = k; j < _cols; j++)
                {
                    a[i][j] -= temp1[m] * a[k][j];
                }
            }
            k++;
        }
        while (k < _rows);

        // /*第二个do-while是将矩阵消成对角形式，并且重新给k赋值,最后只剩下对角线和最后一列的数，其它都为0*/
        k = L - 1;
        do
        {
            n = 0;
            for (l = k; l >= 0; l--)
            {
                temp1[n++] = a[k - l][k + 1] / a[k + 1][k + 1];

            }
            for (m = 0, i = k; i >= 0; i--, m++)
            {
                for (j = k; j < _cols; j++)
                {
                    a[k - i][j] -= temp1[m] * a[k + 1][j];
                }
            }
            k--;
        }
        while (k >= 0);

            /*下一个for是解方程组*/
        double[] newresult = new double[_rows];
        for (i = 0; i < _rows; i++)
        {
            newresult[i] = a[i][_rows] / a[i][i];
        }
        return newresult;
    }

    public static void main(String[] args)
    {

        //2a+4b+1c = 8

        double[][] xishu =
                {
                        {2.0, 4, 1, 8},
                        {4, -2, 2, 0},
                        {4, 1, 0, 6}
                };
        ZgaussianEliminationMatrix2 c = new ZgaussianEliminationMatrix2();
        double[] res = c.calculate(xishu);
        DecimalFormat df = new DecimalFormat("#.00");
        for (double d : res)
        {
            System.out.println(df.format(d));
        }

    }

}
