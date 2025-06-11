import pandas as pd
import numpy as np

class ZCBBuilder:
    def __init__(self, tenor_mths, par_yield_bey):
        self.input_df = pd.DataFrame({
            'MTHS': tenor_mths,
            'PAR_YIELD_BEY': par_yield_bey
        })
        self.input_df['PAR_YIELD_AC'] = self.convert_bey_to_ac(self.input_df['PAR_YIELD_BEY'])

    def convert_bey_to_ac(self, bey_series):  # Convert BEY (semi-annual) to Annual Compounding
        return 1200*((1 + bey_series / 200) ** (1/6) - 1)

    def interpolate_monthly_par_curve(self):  # Monthly timeline up to max tenor
        all_months = np.arange(0, self.input_df['MTHS'].max() + 1)
        curve = pd.DataFrame({'MTHS': all_months})

        # Merge and interpolate
        curve = curve.merge(self.input_df[['MTHS', 'PAR_YIELD_AC']], how='left', on='MTHS')
        curve['INTERP_PAR_YIELD'] = curve['PAR_YIELD_AC'].interpolate(method='linear')
        curve['INTERP_PAR_YIELD'].bfill().ffill() 
        return curve[['MTHS', 'INTERP_PAR_YIELD']]

    def bootstrap_monthly_zcb(self, curve_df):
        zcb = [1.0] # Z(0)
        for i in range(1, len(curve_df)):
            c = curve_df.loc[i, 'INTERP_PAR_YIELD']
            coupon = c / 1200 # Monthly rate
            discounted_coupons = sum(coupon * zcb[j] for j in range(1, i))
            z_i = (1 - discounted_coupons) / (1 + coupon)
            zcb.append(max(z_i, 0.01)) # Apply floor to ZCB

        curve_df['FLOOR_ZCB'] = zcb
        return curve_df[['MTHS', 'INTERP_PAR_YIELD', 'FLOOR_ZCB']]

    def run(self):
        monthly_curve = self.interpolate_monthly_par_curve()
        zcb_df = self.bootstrap_monthly_zcb(monthly_curve)
        return zcb_df


###### FOR TESTING PURPOSES ONLY ######
tenor_mths = [1, 3, 6, 12, 24, 36, 60, 84, 120, 180, 240, 300, 360, 480, 600]
par_yield_bey = [5.0079, 5.0079, 4.8499, 4.7313, 4.5636, 4.6010, 4.6806, 4.8686, 5.0429, 5.1775, 5.3121, 5.2682, 5.2243, 5.2243, 5.2243]

builder = ZCBBuilder(tenor_mths, par_yield_bey)
zcb_output = builder.run()

# Optional: Export to CSV
zcb_output.to_csv("output_zcb.csv", index=False)