const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const geoip = require('geoip-lite');

const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Middleware
app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Geo detection from IP
function getGeoFromIP(ip) {
    if (!ip || ip === '::1' || ip === '127.0.0.1' || ip.startsWith('::ffff:127.')) {
        return { country: 'US', state: 'TX', city: 'Austin' };
    }
    
    let cleanIP = ip;
    if (ip.startsWith('::ffff:')) {
        cleanIP = ip.substring(7);
    }
    
    const geo = geoip.lookup(cleanIP);
    
    if (!geo) {
        return { country: 'US', state: null, city: null };
    }
    
    return {
        country: geo.country || 'US',
        state: geo.region || null,
        city: geo.city || null
    };
}

// Log impression
async function logImpression(data) {
    const query = `
        INSERT INTO impressions (
            ad_id, campaign_id, carrier_id, source, cpm,
            carrier_revenue, adex_revenue, ip, user_agent,
            geo_country, geo_state, geo_city, device_type, shipper, tracking_hash
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id
    `;
    
    const values = [
        data.adId,
        data.campaignId,
        data.carrierId,
        data.source,
        data.cpm,
        data.carrierRevenue,
        data.adexRevenue,
        data.ip,
        data.userAgent,
        data.geo?.country || null,
        data.geo?.state || null,
        data.geo?.city || null,
        data.device || null,
        data.shipper || null,
        data.trackingHash || null
    ];
    
    const result = await pool.query(query, values);
    return result.rows[0].id;
}

// Main ad serving endpoint
app.post('/api/v1/ad', async (req, res) => {
    try {
        const requestData = req.body;
        const { carrier, shipper, device, viewport, trackingHash } = requestData;
        
        const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const geo = getGeoFromIP(clientIp);
        
        console.log('📥 Ad request:', { carrier, shipper, device, geo, trackingHash: trackingHash ? trackingHash.substring(0, 16) + '...' : null });
        
        // Check frequency cap
        if (trackingHash) {
            const frequencyQuery = await pool.query(
                'SELECT COUNT(*) as view_count FROM impressions WHERE tracking_hash = $1',
                [trackingHash]
            );
            
            const viewCount = parseInt(frequencyQuery.rows[0].view_count);
            console.log(`📊 Tracking hash has ${viewCount} previous views`);
            
            if (viewCount >= 5) {
                console.log('⚠️ Frequency cap reached (5 views)');
                return res.json({
                    source: 'google_adsense',
                    fallback: true,
                    reason: 'frequency_cap'
                });
            }
        }
        
        // Find matching campaigns
        const campaignQuery = `
            SELECT c.*, a.name as advertiser_name
            FROM campaigns c
            JOIN advertisers a ON c.advertiser_id = a.id
            WHERE c.status = 'active'
              AND (c.start_date IS NULL OR c.start_date <= NOW())
              AND (c.end_date IS NULL OR c.end_date >= NOW())
              AND (c.budget_remaining IS NULL OR c.budget_remaining > 0)
              AND (c.target_carriers IS NULL OR $1 = ANY(c.target_carriers))
              AND (c.target_geo_countries IS NULL OR $2 = ANY(c.target_geo_countries))
            ORDER BY c.cpm DESC
            LIMIT 10
        `;
        
        const campaigns = await pool.query(campaignQuery, [carrier, geo.country]);
        
        if (campaigns.rows.length === 0) {
            console.log('No campaigns found - returning AdSense');
            return res.json({
                source: 'google_adsense',
                fallback: true,
                reason: 'no_campaigns'
            });
        }
        
        // Filter by shipper targeting
        let matchedCampaigns = campaigns.rows;
        if (shipper) {
            const shipperMatched = campaigns.rows.filter(c => {
                if (!c.target_shippers || c.target_shippers.length === 0) return true;
                return c.target_shippers.some(ts => 
                    shipper.toLowerCase().includes(ts.toLowerCase())
                );
            });
            
            if (shipperMatched.length > 0) {
                matchedCampaigns = shipperMatched;
                console.log(`✓ Shipper targeting matched: ${shipper}`);
            }
        }
        
        const winningCampaign = matchedCampaigns[0];
        console.log(`🎯 Winner: ${winningCampaign.name} at $${winningCampaign.cpm} CPM`);
        
        // Get ad creative
        const adQuery = await pool.query(
            'SELECT * FROM ads WHERE campaign_id = $1 AND status = $2 LIMIT 1',
            [winningCampaign.id, 'active']
        );
        
        if (adQuery.rows.length === 0) {
            console.log('No ad creative found');
            return res.json({
                source: 'google_adsense',
                fallback: true,
                reason: 'no_creative'
            });
        }
        
        const ad = adQuery.rows[0];
        
        // Calculate revenue split
        const cpm = parseFloat(winningCampaign.cpm);
        const revenuePerImpression = cpm / 1000;
        const revenue = {
            total: revenuePerImpression,
            carrier: revenuePerImpression * 0.5,
            adex: revenuePerImpression * 0.5
        };
        
        // Log impression
        const impressionId = await logImpression({
            adId: ad.id,
            campaignId: winningCampaign.id,
            carrierId: carrier,
            source: 'adex_direct',
            cpm: winningCampaign.cpm,
            carrierRevenue: revenue.carrier,
            adexRevenue: revenue.adex,
            ip: clientIp,
            userAgent: req.headers['user-agent'],
            geo: geo,
            device: device,
            shipper: shipper,
            trackingHash: trackingHash
        });
        
        console.log(`✅ Impression logged: ${impressionId}`);
        
        // Return ad
        res.json({
            source: 'adex_direct',
            impressionId: impressionId,
            adId: ad.id,
            campaignId: winningCampaign.id,
            advertiser: winningCampaign.advertiser_name,
            cpm: parseFloat(winningCampaign.cpm),
            html: ad.html_content,
            clickUrl: ad.click_url,
            trackingUrl: `/api/v1/click/${impressionId}`
        });
        
    } catch (error) {
        console.error('❌ Error serving ad:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Click tracking
app.post('/api/v1/click/:impressionId', async (req, res) => {
    try {
        const { impressionId } = req.params;
        
        await pool.query(
            'INSERT INTO clicks (impression_id) VALUES ($1)',
            [impressionId]
        );
        
        console.log(`👆 Click tracked: ${impressionId}`);
        res.json({ success: true });
        
    } catch (error) {
        console.error('❌ Error tracking click:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Start server
app.listen(PORT, () => {
    console.log(`🚀 AdEx API running on port ${PORT}`);
    console.log(`📊 Health check: http://localhost:${PORT}/health`);
});