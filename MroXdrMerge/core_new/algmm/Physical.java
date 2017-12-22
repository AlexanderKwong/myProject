package algmm;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/12/22
 */
public class Physical {


    /**
     * 根据小学一年级数学：
     * 对于 ax^2 + bx + c = y 的抛物线，令y = 0时，x=[-b±√(b^2-4ac)]/(2a)
     * 由初一物理有位移公式：s = v0 * _t + 1/2 * a * _t^2 (其中v0为初速度，a为加速度)
     * 给出匀加速直线运动的两点(v1, t1), (v2, t2) 有：  v1 * _t + 1/2 (_t ^2) * (v2 - v1)/(t2 - t1) = _s
     * 仅保留正解
     * @param v1 速度1
     * @param v2 速度2
     * @param t1 时间1
     * @param t2 时间2 , t2 > t1
     * @param _s 位移
     * @return _t 时间差
     */
    public static double compute(double v1, double v2, long t1, long t2, double _s){

        if(v1 < 0 || v2 < 0 || t1 < 0 || t2 < 0 || _s < 0){
            throw new IllegalArgumentException();
        }

        double a = ( v2 - v1 )/ 2 * (t2 - t1);

        double b = v1;

        double c = - _s;

        //判别式△ =  b^2-4ac
        double judge = Math.pow(b, 2) - 4 * a * c;

        if(judge <= 0 || judge < b){
            throw new IllegalArgumentException();
        }

        return (judge - b) / (2 * a);
    }

    public static void main(String[] args){
        /**
         * 对每个定位点：
         *     while(跟前一个点的距离 _s >= 200)
         *          _t = compute(v1, v2, t1, t2, 200)
         *          _s = _s - 200
         *          t0 = t0 + _t
         *          seg.setTime(t0)
         *
         *     _s = 200 - _s
         */

    }
}
