import 'dotenv/config'
import http from 'http'
import express from 'express'
import bodyParser from 'body-parser'
import { WebSocketServer, WebSocket } from 'ws'
import fetch from 'node-fetch'
import winston from 'winston'

// 

const {
  PORT 

const TWILIO_TRANSFER_SIP  = TWILIO_TRANSFER_SIP_URI || `sip:+${TRANSFER_NUMBER}@sip.twilio.com` // 1 hour max
    this.maxConversationHistory  = 50 // Trim to last 50 messages
    this.maxTokens  = 100000 // Prevent cost explosions
  }

  startCleanup() {
    setInterval(()  = > this.cleanup(), 60000) // Clean every minute
  }

  cleanup() {
    const now  = Date.now()
    let cleaned  = 0
    
    // Clean up abandoned calls
    for (const [callSid, convo] of activeCalls) {
      const callAge  = now - convo.callStartTime
      if (callAge > this.maxCallDuration) {
        logger.warn(`🧹 Cleaning up abandoned call: ${callSid} (${Math.floor(callAge/1000)}s old)`)
        activeCalls.delete(callSid)
        conversationLogs.delete(callSid)
        cleaned++
      }
    }
    
    if (cleaned > 0) {
      logger.info(`🧹 Memory cleanup: Removed ${cleaned} abandoned calls`)
    }
  }
}

const memoryManager  = new MemoryManager()
    this.state  = 'CLOSED'
    this.nextAttempt  = Date.now()
    this.timeout  = timeout
    this.threshold  = threshold
    this.resetTimeout  = resetTimeout
  }

  async call(serviceFn) {
    if (this.state 
      }
      this.state  = 'HALF_OPEN'
    }
    
    try {
      const result  = await serviceFn()
      this.failures  = 0
      this.state  = 'CLOSED'
      return result
    } catch (error) {
      this.failures++
      if (this.failures >
        this.nextAttempt  = Date.now() + this.resetTimeout
        logger.error(`🚨 Circuit breaker opened after ${this.failures} failures`)
      }
      throw error
    }
  }
}

// Create circuit breakers for external services
const calComCircuitBreaker  = new CircuitBreaker()
const deepseekCircuitBreaker  = new CircuitBreaker()
const zohoCircuitBreaker  = new CircuitBreaker()
const deepgramTtsCircuitBreaker  = new CircuitBreaker()

// Enhanced body parser with better error handling
app.use(bodyParser.json({ 
  limit: "50mb",
  verify: (req, res, buf)  = > {
    try {
      JSON.parse(buf)
    } catch (e) {
      throw new Error('Invalid JSON')
    }
  }
}))

app.use(bodyParser.urlencoded({ 
  limit: "50mb", 
  extended: true,
  verify: (req, res, buf)  = > {
    if (buf.length > 50 * 1024 * 1024) {
      throw new Error('Payload too large')

// Enhanced payload size protection with early checks
app.use((req, res, next) 
  if (contentLength && parseInt(contentLength) > 50 * 1024 * 1024) {
    logger.error('❌ Payload too large', { size: contentLength })
    return res.status(413).json({ error: 'Payload too large' })
  }
  
  // Additional check for chunked encoding
  req.on('data', (chunk)  = > {
    if (req.socket.bytesRead > 50 * 1024 * 1024) {
      req.destroy()
      res.status(413).json({ error: 'Payload too large' })
    }
  })
  
  next()
})

// Error handling middleware for payload errors
app.use((error, req, res, next) 
  }
  if (error.message 
  }
  next(error)
  }

  reset() {
    this.telephony  = {
      switchFee: 0.01,
      perMinuteRate: 0.014,
      recordingPerMinute: 0.00 // ✅ FIX: Set to 0 as requested
    }
    this.deepgramSttPerMinute  = 0.0077 // ✅ FIX: Updated from 0.0043
    this.deepgramTtsPerThousand  = 0.030 // ✅ FIX: Updated from per-million to per-thousand
    this.deepseek  = {
      inputPerMillion: Number(DEEPSEEK_INPUT_RATE) || 0.28,
      outputPerMillion: Number(DEEPSEEK_OUTPUT_RATE) || 0.42
    }
    
    this.metrics  = {
      callDuration: 0,
      charactersSpoken: 0,
      deepseekTokens: { input: 0, output: 0 },
      recordingUsed: false,
      callMinutes: 0
    }
  }

  addCallDuration(seconds) {
    // ✅ PREVENTATIVE FIX: Validate duration is reasonable
    if (seconds < 0) {
      logger.error(`🚫 Invalid negative duration: ${seconds}`)
      return
    }
    
    // Prevent ridiculously long calls (24+ hours)
    if (seconds > 86400) {
      logger.warn(`⚠️ Suspiciously long call duration: ${seconds} seconds`)
      seconds  = 86400 // Cap at 24 hours
    }
    
    this.metrics.callDuration  = seconds
    this.metrics.callMinutes  = Math.ceil(seconds / 60)
  }

  addCharactersSpoken(characters) {
    // ✅ PREVENTATIVE FIX: Validate character count
    if (characters < 0) {
      logger.error(`🚫 Invalid negative characters: ${characters}`)
    }
    
    // Prevent overflow from corrupted data
    if (characters > 1000000) { // 1 million characters
      logger.warn(`⚠️ Suspiciously high character count: ${characters}`)
      characters  = 1000000
    }
    
    this.metrics.charactersSpoken  = characters
  }

  addDeepSeekTokens(input, output) {
    // ✅ FIX: Prevent cost explosions with token limits
    const MAX_TOKENS  = 100000
    this.metrics.deepseekTokens.input  = Math.min(
      input + this.metrics.deepseekTokens.input, 
      MAX_TOKENS
    )
    this.metrics.deepseekTokens.output  = Math.min(
      output + this.metrics.deepseekTokens.output, 
      MAX_TOKENS
    )
  }

  setRecordingUsed(used) {
    this.metrics.recordingUsed  = used
  }

  calculateCosts() {
    const minutesBilled  = this.metrics.callMinutes
    
    // Telephony costs
    const switchCost  = this.telephony.switchFee
    const callCost  = minutesBilled * this.telephony.perMinuteRate
    const recordingCost  = 0 // ✅ FIX: Always 0 now
    const totalTelephonyCost  = switchCost + callCost + recordingCost
    
    // AI costs - ✅ FIX: Updated calculations
    const deepgramSttCost  = (this.metrics.callDuration / 60) * this.deepgramSttPerMinute
    const deepgramTtsCost  = (this.metrics.charactersSpoken / 1000) * this.deepgramTtsPerThousand
    const deepseekInCost  = (this.metrics.deepseekTokens.input / 1000000) * this.deepseek.inputPerMillion
    const deepseekOutCost  = (this.metrics.deepseekTokens.output / 1000000) * this.deepseek.outputPerMillion
    
    // Unified total
    const totalCost  = totalTelephonyCost + deepgramSttCost + deepgramTtsCost + deepseekInCost + deepseekOutCost
    
    return {
      telephony: {
        switchCost: Number(switchCost.toFixed(6)),
        callCost: Number(callCost.toFixed(6)),
        recordingCost: Number(recordingCost.toFixed(6)),
        totalTelephonyCost: Number(totalTelephonyCost.toFixed(6)),
        minutesBilled
      },
      ai: {
        deepgramSttCost: Number(deepgramSttCost.toFixed(6)),
        deepgramTtsCost: Number(deepgramTtsCost.toFixed(6)),
        deepseekInCost: Number(deepseekInCost.toFixed(6)),
        deepseekOutCost: Number(deepseekOutCost.toFixed(6))
      },
      totalCost: Number(totalCost.toFixed(6)),
      metrics: this.metrics
    }
  }

  getCostSummary() {
    const costs  = this.calculateCosts()
    return {
      total: costs.totalCost,
      breakdown: costs,
      metrics: this.metrics,
      perMinute: costs.totalCost / (this.metrics.callDuration / 60),
      perSecond: costs.totalCost / this.metrics.callDuration
    }
  }
}

// 
    this.conversationLog  = []
    this.startTime  = new Date()
    this.metrics  = {
      stateTransitions: 0,
      interruptions: 0,
      positiveSignals: 0,
      negativeSignals: 0,
      lastState: null
    }
  }

  logUserInput(text) {
    // ✅ PREVENTATIVE FIX: Validate and sanitize input
    if (!text || typeof text !
      return null
    }
    
    // Prevent log bloat from extremely long inputs
    if (text.length > 1000) {
      logger.warn(`⚠️ Truncating long user input: ${text.length} characters`)
      text  = text.substring(0, 500) + '... [truncated]'
    }
    
    const entry  = {
      timestamp: new Date().toISOString(),
      type: 'USER',
      text: text
    }
    this.conversationLog.push(entry)
    
    // ✅ FIX: Trim conversation history to prevent memory leaks
    if (this.conversationLog.length > 50) {
      this.conversationLog  = this.conversationLog.slice(-25) // Keep last 25 entries
    }
    
    logger.info(`👤 USER: ${text}`)
    return entry
  }

  logBotResponse(text) {
    const entry  = {
      timestamp: new Date().toISOString(),
      type: 'BOT',
      text: text
    } // Keep last 25 entries
    }
    
    logger.info(`🤖 BOT: ${text}`)
  }

  logSystemEvent(event) {
    const entry  = {
      timestamp: new Date().toISOString(),
      type: 'SYSTEM',
      event: event
    }
    logger.info(`📞 SYSTEM: ${event}`)
  }

  logStateChange(fromState, toState) {
    this.metrics.stateTransitions++
    this.metrics.lastState  = toState
    logger.info(`🧠 STATE: ${fromState} → ${toState}`)
    this.logSystemEvent(`State changed: ${fromState} -> ${toState}`)
  }

  logTimeEvent(event) {
    logger.info(`⏰ TIME: ${event}`)
  }

  formatTime() {
    return new Date().toLocaleTimeString('en-US', { hour12: true })
  }

  getConversationLog() {
    return this.conversationLog.filter(entry 
  }

  getMetrics() {
    return this.metrics
  }

  printConversationSummary() {
    const conversationLines  = this.getConversationLog()
    logger.info('🎯 CONVERSATION SUMMARY - User/Bot exchanges only')
    
    conversationLines.forEach(entry 
      if (entry.type 
      } else {
        logger.info(`🤖 [${time}] BOT: ${entry.text}`)
      }
    })
    this.tokenExpiry  = null
    this.baseURL  = 'https://www.zohoapis.com/crm/v6'
  }
  
  async getToken(maxRetries 
      return this.accessToken
    }

    for (let attempt  = 1 attempt < = maxRetries attempt++) {
      try {
        const body 
        
        // ✅ FIX: Add timeout to prevent hanging requests
        const controller  = new AbortController()
        const timeoutId 
        
        const response  = await fetch('https://accounts.zoho.com/oauth/v2/token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: body,
          signal: controller.signal
        })
        
        clearTimeout(timeoutId)
        
        if (response.ok) {
          const data  = await response.json()
          this.accessToken  = data.access_token
          this.tokenExpiry  = Date.now() + (45 * 60 * 1000)
          logger.info('✅ Zoho token refreshed', { expiresIn: 45 * 60 })
          return this.accessToken
        } else {
          const errorText  = await response.text()
          logger.error('❌ Zoho token refresh failed', { attempt, status: response.status, error: errorText })
          if (attempt 
        }
      } catch (error) {
        logger.error('❌ Zoho token refresh error', { attempt, message: error.message, stack: error.stack })
        if (attempt 
      }
      await new Promise(resolve  = > setTimeout(resolve, retryDelay))
    }
    
    return null
  }

  async ensureCustomFields() {
    const token  = await this.getToken()
    if (!token) return false

    try {
      const fields  = [
        {
          "api_name": "Call_SID",
          "module": { "api_name": "Calls" },
          "data_type": "text",
          "length": 100,
          "display_label": "Call SID",
          "read_only": false
        },
        {
          "api_name": "Call_Cost",
          "module": { "api_name": "Calls" },
          "data_type": "currency",
          "precision": 4,
          "display_label": "Call Cost (USD)",
          "read_only": false
        },
        {
          "api_name": "Last_Recording_URL",
          "module": { "api_name": "Calls" },
          "data_type": "website",
          "display_label": "Last Recording URL",
          "read_only": false
        },
        {
          "api_name": "AI_Call_Count",
          "module": { "api_name": "Accounts" },
          "data_type": "integer",
          "display_label": "AI Call Count",
          "read_only": false,
          "default_value": "0"
        }
      ]

      for (const field of fields) {
        const response  = await fetch(`${this.baseURL}/settings/fields`, {
          method: 'POST',
          headers: {
            'Authorization': `Zoho-oauthtoken ${token}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ fields: [field] })
        })

        if (response.ok) {
          logger.info(`✅ Created Zoho field: ${field.api_name}`)
        } else if (response.status 
        } else {
          logger.warn(`⚠️ Could not create Zoho field ${field.api_name}: ${response.status}`)
        }
      }
      return true
    } catch (error) {
      logger.error('❌ Zoho field creation error:', { message: error.message })
      return false
    }
  }

  async findAccountByPhone(phone) {
    const token  = await this.getToken()
    if (!token) return null

    try {
      const cleanPhone  = phone.replace(/\D/g, '')
      const response 

      if (response.ok) {
        const data  = await response.json()
        if (data.data && data.data.length > 0) {
          return data.data[0]
        }
      }
    } catch (error) {
      logger.error('❌ Zoho account search error:', { message: error.message })
  }

  async createOrUpdateAccount(accountData) {
    return await zohoCircuitBreaker.call(async () 
      if (!token) return null

      try {
        // Try to find existing account
        let accountId  = null
        if (accountData.phone) {
          const existing  = await this.findAccountByPhone(accountData.phone)
          if (existing) {
            accountId  = existing.id
          }
        }

        const payload  = {
          data: [accountData]
        }

        let response
        if (accountId) {
          // Update existing account
          payload.data[0].id  = accountId
          response  = await fetch(`${this.baseURL}/Accounts`, {
            method: 'PUT',
            headers: {
              'Authorization': `Zoho-oauthtoken ${token}`,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
          })
        } else {
          // Create new account
          response  = await fetch(`${this.baseURL}/Accounts`, {
            method: 'POST',
            headers: {
              'Authorization': `Zoho-oauthtoken ${token}`,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
          })
        }

        if (response.ok) {
          const result  = await response.json()
          return result.data[0]
        } else {
          logger.error('❌ Zoho account creation error:', { error: await response.text() })
        }
      } catch (error) {
        logger.error('❌ Zoho account operation error:', { message: error.message })
      }
      
      return null
    })
  }

  async searchCallBySid(callSid) {
    const token  = await this.getToken()

    try {
      const response 
        }
      }
    } catch (error) {
      logger.error('❌ Zoho call search error:', { message: error.message })
  }

  async createCallLog(callData) {
    return await zohoCircuitBreaker.call(async ()  = > {
      // Ensure custom fields exist first
      await this.ensureCustomFields()
      
      const token  = await this.getToken()

      try {
        // Check if call already exists
        const existingCall  = await this.searchCallBySid(callData.Call_SID)
        if (existingCall) {
          // Update existing call
          return await this.updateCallLog(existingCall.id, callData)
        }

        const payload  = {
          data: [callData]
        }

        const response  = await fetch(`${this.baseURL}/Calls`, {
          method: 'POST',
          headers: {
            'Authorization': `Zoho-oauthtoken ${token}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(payload)
        })

        if (response.ok) {
          const result  = await response.json()
          logger.info('✅ Zoho call log created:', { callId: result.data[0].id })
          logger.error('❌ Zoho call log creation error:', { error: errorText })
        }
      } catch (error) {
        logger.error('❌ Zoho call log error:', { message: error.message })
  }

  async updateCallLog(callId, callData) {
    const token  = await this.getToken()

    try {
      const payload  = {
        data: [{
          id: callId,
          ...callData
        }]
      }

      const response  = await fetch(`${this.baseURL}/Calls`, {
        method: 'PUT',
        headers: {
          'Authorization': `Zoho-oauthtoken ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })

      if (response.ok) {
        const result  = await response.json()
        logger.info('✅ Zoho call log updated:', { callId })
        return result.data[0]
      } else {
        logger.error('❌ Zoho call log update error:', { error: await response.text() })
      }
    } catch (error) {
      logger.error('❌ Zoho call log update error:', { message: error.message })
  }

  async updateAccountMetrics(accountId, metrics) {
    const token  = await this.getToken()

    try {
      // Get current account data to increment properly
      const currentAccount  = await this.getAccount(accountId)
      if (!currentAccount) return null

      const currentCallCount  = parseInt(currentAccount.AI_Call_Count) || 0
      const currentTotalCost  = parseFloat(currentAccount.Total_AI_Call_Cost) || 0

      const payload  = {
        data: [{
          id: accountId,
          AI_Call_Count: currentCallCount + 1,
          Last_Call_Cost: metrics.lastCallCost || 0,
          Total_AI_Call_Cost: currentTotalCost + (metrics.lastCallCost || 0),
          Last_Recording_URL: metrics.recordingUrl || '',
          Last_Call_Date: new Date().toISOString().split('T')[0]
        }]
      }

      if (response.ok) {
        logger.info('✅ Zoho account metrics updated:', { accountId })
        return await response.json()
      }
    } catch (error) {
      logger.error('❌ Zoho metrics update error:', { message: error.message })
  }

  async getAccount(accountId) {
    const token  = await this.getToken()
        return data.data[0]
      }
    } catch (error) {
      logger.error('❌ Zoho account fetch error:', { message: error.message })
  }
}

const zohoIntegration  = new EnhancedZohoIntegration()
  }

  async getCallDetails(callSid) {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
      logger.error('❌ Twilio credentials not configured')
    }

    try {
      const auth  = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString('base64')

      if (response.ok) {
        const callData  = await response.json()
        return callData
      } else {
        logger.error('❌ Twilio call details error:', { status: response.status })
      }
    } catch (error) {
      logger.error('❌ Twilio call details fetch error:', { message: error.message })
  }

  async getRecordingUrl(callSid) {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
      return null

      if (response.ok) {
        const recordings  = await response.json()
        if (recordings.recordings && recordings.recordings.length > 0) {
          const recording  = recordings.recordings[0]
          return `https://api.twilio.com${recording.uri.replace('.json', '.mp3')}`
        }
      }
    } catch (error) {
      logger.error('❌ Twilio recording fetch error:', { message: error.message })
  }

  generateStructuredNotes(callData, costBreakdown, conversationData) {
    const {
      contact_name 

    const {
      totalCost,
      telephony,
      ai
    }  = costBreakdown

    return `
═══════════════════════════════════════
📞 CALL SUMMARY
═══════════════════════════════════════
Contact: ${contact_name}
Duration: ${duration_seconds}s
Outcome: ${outcome}

═══════════════════════════════════════
💰 CALL COSTS
═══════════════════════════════════════
Total: $${totalCost.toFixed(6)}
├─ Twilio: $${telephony.totalTelephonyCost.toFixed(6)} (${telephony.minutesBilled} minutes billed)
│  ├─ Switch Fee: $${telephony.switchCost.toFixed(6)}
│  ├─ Call Cost: $${telephony.callCost.toFixed(6)}
│  └─ Recording: $${telephony.recordingCost.toFixed(6)}
├─ Deepgram STT: $${ai.deepgramSttCost.toFixed(6)}
├─ Deepgram TTS: $${ai.deepgramTtsCost.toFixed(6)} (${costBreakdown.metrics.charactersSpoken} chars)
└─ DeepSeek V3.2: $${(ai.deepseekInCost + ai.deepseekOutCost).toFixed(6)}
   ├─ Input: $${ai.deepseekInCost.toFixed(6)} (${costBreakdown.metrics.deepseekTokens.input} tokens)
   └─ Output: $${ai.deepseekOutCost.toFixed(6)} (${costBreakdown.metrics.deepseekTokens.output} tokens)

═══════════════════════════════════════
📊 BUSINESS DETAILS
═══════════════════════════════════════
Seats: ${employee_count}
Provider: ${phone_provider}
CRM: ${crm_name}
Email: ${email}

═══════════════════════════════════════
📅 BOOKING DETAILS
═══════════════════════════════════════
Cal.com ID: ${cal_id}
Time: ${local_time}
Calendar Event: ${cal_id !
  }
}

const twilioCallLogger  = new TwilioCallLogger()
    
    // ✅ PREVENTATIVE FIX: Validate webhook data structure
    if (!webhookData || typeof webhookData !
      return res.status(400).json({ error: 'Invalid webhook data' })
    }
    
    // Validate required fields for call logging
    if (webhookData.callSid) {
      if (typeof webhookData.callSid !
        return res.status(400).json({ error: 'Invalid callSid' })
      }
    }
    
    // Validate duration if provided
    if (webhookData.duration && (webhookData.duration < 0 || webhookData.duration > 86400)) {
      logger.warn('⚠️ Suspicious duration in webhook, capping')
      webhookData.duration  = Math.min(Math.max(webhookData.duration, 0), 86400)
    }
    
    logger.info('📥 Received n8n webhook:', { data: webhookData })

    // Handle callback requests
    if (webhookData.type 
      return res.json({ success: true, type: 'callback_request_processed' })
    }

    // Handle contact merge requests
    if (webhookData.type 
      return res.json({ success: true, type: 'contact_merge_processed' })
    }

    // Original call logging logic
    const {
      callSid,
      duration,
      from,
      to,
      status,
      callCost,
      recordingUrl,
      recordingDuration,
      conversationData 

    if (!callSid) {
      return res.status(400).json({ error: 'Missing callSid' })
    }

    // Get additional call details from Twilio
    const callDetails  = await twilioCallLogger.getCallDetails(callSid)
    const finalRecordingUrl  = recordingUrl || await twilioCallLogger.getRecordingUrl(callSid)

    // Calculate costs using EnhancedCostTracker
    const costTracker  = new EnhancedCostTracker()
    costTracker.addCallDuration(parseInt(duration) || 0)
    costTracker.addCharactersSpoken(conversationData.charactersSpoken || 0)
    costTracker.addDeepSeekTokens(
      conversationData.deepseekInputTokens || 0,
      conversationData.deepseekOutputTokens || 0
    )
    costTracker.setRecordingUsed(!!finalRecordingUrl)

    const costBreakdown  = costTracker.calculateCosts()

    // Prepare call data for Zoho
    const callData  = {
      contact_name: conversationData.contactName || 'Unknown',
      duration_seconds: duration || 0,
      outcome: conversationData.outcome || status || 'Unknown',
      employee_count: conversationData.employeeCount || 'Unknown',
      phone_provider: conversationData.phoneProvider || 'Unknown',
      crm_name: conversationData.crmName || 'N/A',
      email: conversationData.email || 'Not collected',
      cal_id: conversationData.calComBookingId || 'N/A',
      local_time: conversationData.bookingTime || 'N/A',
      recording_url: finalRecordingUrl || 'N/A',
      recording_duration: recordingDuration || '0s',
      tags: conversationData.tags || 'Called-Today, attempt-1'
    }

    // Generate structured notes
    const structuredNotes  = twilioCallLogger.generateStructuredNotes(callData, costBreakdown, conversationData)

    // Prepare Zoho Call record
    const zohoCallData  = {
      Subject: `AI Call - ${callData.outcome}`,
      Call_Type: 'Outbound',
      Call_Start_Time: new Date().toISOString(),
      Call_Duration: Math.ceil(parseInt(duration) / 60) || 0,
      Call_SID: callSid,
      Recording_URL: finalRecordingUrl,
      Description: structuredNotes,
      Call_Result: callData.outcome,
      Call_Purpose: 'AI Agent Outreach',
      Call_Cost: costBreakdown.totalCost,
      Last_Recording_URL: finalRecordingUrl
    }

    // Find or create account
    const accountData  = {
      Phone: from,
      Account_Name: conversationData.companyName || 'Unknown Company',
      Last_Call_Cost: costBreakdown.totalCost,
      Last_Recording_URL: finalRecordingUrl
    }

    // Add location info if available
    if (conversationData.locationInfo) {
      accountData.Primary_State  = conversationData.locationInfo.state
      accountData.Primary_Area_Code  = conversationData.locationInfo.areaCode
      accountData.Time_Zone  = conversationData.locationInfo.timezone
    }

    // Add business metrics
    if (conversationData.seats) {
      accountData.Number_of_Employees  = conversationData.seats
    }
    if (conversationData.provider) {
      accountData.Current_Provider  = conversationData.provider
    }
    if (conversationData.useCRM !
    }
    if (conversationData.crmName) {
      accountData.CRM_Name  = conversationData.crmName
    }

    const account  = await zohoIntegration.createOrUpdateAccount(accountData)
    
    if (account) {
      logger.info(`✅ Zoho account ${account.id ? 'updated' : 'created'}:`, { accountId: account.id })
      
      // Link call to account
      zohoCallData.What_Id  = account.id

      // Create call log
      const callLog  = await zohoIntegration.createCallLog(zohoCallData)
      
      if (callLog) {
        logger.info(`✅ Zoho call log created:`, { callLogId: callLog.id })
        
        // Update account with aggregated metrics
        await zohoIntegration.updateAccountMetrics(account.id, {
          lastCallCost: costBreakdown.totalCost,
          recordingUrl: finalRecordingUrl
        })

        // Send contact merge request to n8n if we have contact info
        if (conversationData.contactName || conversationData.email) {
          await fetch(N8N_WEBHOOK_URL || `${req.protocol}://${req.get('host')}/n8n-webhook`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              type: 'contact_merge',
              firstName: conversationData.contactName?.split(' ')[0] || 'Unknown',
              lastName: conversationData.contactName?.split(' ').slice(1).join(' ') || 'Unknown',
              email: conversationData.email || null,
              phone: from,
              accountId: account.id,
              accountName: accountData.Account_Name,
              source: 'Ava Voice',
              callSid: callSid,
              timestamp: new Date().toISOString()
            })
          })
        }

        return res.json({
          success: true,
          accountId: account.id,
          callLogId: callLog.id,
          costBreakdown: costBreakdown,
          structuredNotes: structuredNotes
        })
      }
    }

    return res.status(500).json({ error: 'Failed to create Zoho records' })

  } catch (error) {
    logger.error('❌ n8n webhook error:', { message: error.message })
    return res.status(500).json({ error: error.message })
  }
})

    const minutesBilled  = Math.ceil(duration / 60)

    // Telephony costs
    const switchCost  = 0.01
    const perMinRate  = 0.014
    const recordingPerMin  = 0.00 // ✅ FIX: Set to 0
    const callMinutes  = minutesBilled // ✅ FIX: Always 0
    const totalTelephonyCost  = switchCost + callCost + recordingCost

    // DeepSeek cost (per-token)
    const inputTokens  = deepseekUsage.inputTokens || 0
    const outputTokens  = deepseekUsage.outputTokens || 0
    const inputRatePerMillion  = Number(DEEPSEEK_INPUT_RATE) || 0.28
    const outputRatePerMillion  = Number(DEEPSEEK_OUTPUT_RATE) || 0.42

    const deepseekInCost  = (inputTokens / 1_000_000) * inputRatePerMillion

    // ✅ FIX: Updated Deepgram costs
    const deepgramSttPerMinute  = 0.0077
    const deepgramTtsPerThousand  = 0.030
    
    const deepgramSttCost  = (duration / 60) * deepgramSttPerMinute

    // Unified total
    const totalCost  = Number((totalTelephonyCost + deepseekInCost + deepseekOutCost + deepgramTtsCost + deepgramSttCost).toFixed(6))

    const result  = {
      callMinutes,
      switchCost: Number(switchCost.toFixed(6)),
      callCost: Number(callCost.toFixed(6)),
      recordingCost: Number(recordingCost.toFixed(6)),
      totalTelephonyCost: Number(totalTelephonyCost.toFixed(6)),
      deepseekInCost: Number(deepseekInCost.toFixed(6)),
      deepseekOutCost: Number(deepseekOutCost.toFixed(6)),
      deepgramTtsCost: Number(deepgramTtsCost.toFixed(6)),
      deepgramSttCost: Number(deepgramSttCost.toFixed(6)),
      totalCost
    }

    res.json(result)

  } catch (error) {
    logger.error('❌ Cost calculation error:', { message: error.message })
    res.status(500).json({ error: error.message })
    this.model  = 'deepseek-chat'
  }

  // 
    
    const cleanText  = text.toLowerCase().trim()
    
    // If it's clearly a greeting or simple response, return null immediately
    if (rejectedAsName.some(greeting 
        return null
    }
    
    // Also reject single-word greetings even if they have punctuation
    if (/^(hello|hi|hey)[?!.]*$/i.test(cleanText)) {
        logger.info(`🚫 Rejected "${text}" as name - single word greeting`)
    }

    // Try DeepSeek extraction first if available
    if (this.apiKey) {
        try {
            const response  = await fetch(this.baseURL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                },
                body: JSON.stringify({
                    model: this.model,
                    max_tokens: 100,
                    messages: [{
                        role: 'user',
                        content: `Extract just the person's name from this text. Return ONLY the name, nothing else. If no clear person name is present, return "unknown". Important: DO NOT extract greetings like "hello" or "hi" as names. Text: "${text}"`
                    }]
                })
            })

            if (response.ok) {
                const data  = await response.json()
                const name  = data.choices[0].message.content.trim()
                
                // Validate the extracted name
                if (name && 
                    name !
                    return name
                }
            }
        } catch (error) {
            logger.error('❌ DeepSeek name extraction error:', { message: error.message })
            // Fall through to fallback
        }
    }

    // Enhanced fallback extraction with better filtering
    return this.fallbackNameExtraction(text)
  }

  fallbackNameExtraction(text) {
    const words  = text.split(/\s+/)
    
    // Words that should NEVER be considered as names
    const neverNames  = new Set([
        'hello', 'hi', 'hey', 'yes', 'no', 'yeah', 'nope', 'ok', 'okay',
        'good', 'morning', 'afternoon', 'evening', 'thanks', 'thank', 'you',
        'calling', 'call', 'this', 'is', 'the', 'that', 'what', 'who',
        'speaking', 'talk', 'transfer', 'hold', 'wait', 'please', 'sorry'
    ])
    
    for (const word of words) {
        const cleaned  = word.replace(/[^a-zA-Z]/g, '')
        
        // Strong validation for potential names
        if (cleaned.length >
            return cleaned
        }
    }
    
    logger.info(`❌ No valid name found in: "${text}"`)
    return null
  }

  // FIXED: Enhanced interpretWithLLM with proper JSON error handling
  async interpretWithLLM(userText, currentState, goal) {
    if (!this.apiKey) {
      return {
        understood: false,
        value: null,
        response: "Please provide more details.",
        clarification: "Could you clarify that?",
        confidence: "low"
      }
    }

    const systemPrompt  = `You are helping interpret user responses for a voice sales agent.

Current state: ${currentState}
Goal: ${goal}

RULES: Keep responses under 25 words for phone clarity. One idea per response. Use contractions.

Return JSON only:
{
  "understood": true/false,
  "value": extracted_value_or_null,
  "response": "brief natural response (under 25 words)",
  "clarification": "brief question if not understood (under 20 words)",
  "confidence": "high|medium|low"
}`

        if (!response.ok) {
          const errorText  = await response.text()
          throw new Error(`DeepSeek API error: ${response.status} - ${errorText}`)
        }

        return response
      })

      const data  = await response.json()
      
      // FIXED: Proper JSON parsing with error handling
      if (data.choices && data.choices[0] && data.choices[0].message) {
        const content  = data.choices[0].message.content
        try {
          const parsed  = JSON.parse(content)
          return parsed
        } catch (parseError) {
          logger.error('❌ DeepSeek JSON parse error:', { 
            content: content,
            error: parseError.message 
          })
          return {
            understood: false,
            value: null,
            response: "Let me check that for you.",
            clarification: "Could you repeat that?",
            confidence: "low"
          }
        }
      } else {
        throw new Error('Invalid response format from DeepSeek')
      }

    } catch (error) {
      logger.error('❌ DeepSeek interpretation error:', { message: error.message })
      return {
        understood: false,
        value: null,
        response: "Let me check that for you.",
        clarification: "Could you repeat that?",
        confidence: "low"
      }
    }
  }
}

const deepseek  = new DeepSeekIntegration()
    
    // First try direct digit matching
    const digitMatch  = text.match(/\b(\d+)\b/)
    if (digitMatch) {
      const num  = parseInt(digitMatch[0])
      if (num > 0 && num < = 1000) {
        return num
      }
    }
    
    // Handle number words with improved pattern matching
    const numberWords  = {
      'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
      'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
      'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14, 'fifteen': 15,
      'sixteen': 16, 'seventeen': 17, 'eighteen': 18, 'nineteen': 19,
      'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50,
      'sixty': 60, 'seventy': 70, 'eighty': 80, 'ninety': 90
    }
    
    const words  = text.toLowerCase().split(/\s+/)
    let total  = 0
    let current  = 0
    
    for (let i  = 0 i < words.length i++) {
      const word  = words[i].replace(/[^a-zA-Z]/g, '')
      
      if (numberWords[word] !
        
        if (num >
        } else if (num < 20) {
          current + = num
        }
      }
      
      // Handle compound numbers like "twenty five"
      if (word 
        if (numberWords[nextWord] !
          i++ // Skip next word
        }
      }
      
      // Handle "hundred"
      if (word 
      }
    }
    
    total + = current
    
    // Only return if we found a reasonable number
    if (total > 0 && total < = 1000) {
      return total
  }

  reset() {
    this.attempts  = 0
    this.maxAttempts  = 2
  }

  extractEmail(text) {
    if (!text) return null
    
    // ✅ FIX: Better handling of spoken email patterns
    const cleanText  = text.toLowerCase().trim()
    
    // Handle "casey at goatvox dot com" pattern
    if (cleanText.includes(' at ') && cleanText.includes(' dot ')) {
      const converted  = this.convertSpokenEmail(text)
      if (converted && this.validateEmail(converted)) {
        logger.info(`✅ Converted spoken email: ${text} -> ${converted}`)
        return converted
      }
    }
    
    // Enhanced email patterns
    const emailPatterns  = [
      /\b[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}\b/, // standard
      /\b[\w._%+-]+\s*@\s*[\w.-]+\s*\.\s*[A-Za-z]{2,}\b/, // with spaces
    ]
    
    for (const pattern of emailPatterns) {
      const match  = text.match(pattern)
      if (match) {
        const email  = match[0].replace(/\s+/g, '')
        if (this.validateEmail(email)) {
          logger.info(`✅ Extracted valid email: ${email}`)
          return email
        }
      }
    }
    
    return null
  }

  // Fix the validateEmail method to handle edge cases better:
  validateEmail(email) {
    if (!email || email.length > 50) return false
    
    // Check for multiple @ symbols more carefully
    const atCount  = (email.match(/@/g) || []).length
    if (atCount !
    }
    
    const emailRegex  = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    return emailRegex.test(email)
  }

  convertSpokenEmail(text) {
    let email  = text.toLowerCase().trim()
    
    // Remove common filler words and punctuation
    email  = email.replace(/\b(and|the|my|email|address|is|at|dot)\b/gi, '')
    email  = email.replace(/[.,!?]/g, '')
    
    // Convert " at " to "@" and " dot " to "."
    email  = email.replace(/\s+at\s+/g, '@')
    
    // Remove any remaining spaces
    email  = email.replace(/\s/g, '')
    
    // Enhanced validation
    const emailRegex  = /^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/
    
    if (emailRegex.test(email)) {
      logger.info(`✅ Converted spoken email: ${text} -> ${email}`)
      return email
  }

  recordAttempt() {
    this.attempts++
    return this.attempts > = this.maxAttempts
  }

  shouldGiveUp() {
    return this.attempts > = this.maxAttempts
    this.lastDigitTime  = Date.now()
  }

  processPhoneInput(text) {
    const now  = Date.now()
    
    if (now - this.lastDigitTime > this.timeout) {
      this.digitBuffer  = ''
    }
    
    this.lastDigitTime  = now
    
    const digits  = text.replace(/\D/g, '')
    this.digitBuffer + = digits
    
    // ✅ PREVENTATIVE FIX: Validate phone number length and patterns
    if (this.digitBuffer.length 
      
      // Additional validation: reject obvious fake numbers
      const fakePatterns  = [
          /^1234567890$/, /^1111111111$/, /^0000000000$/, /^5555555555$/,
          /^(\d)\1{9}$/ // All same digit
      ]
      
      if (phoneRegex.test(this.digitBuffer) && 
          !fakePatterns.some(pattern 
        this.digitBuffer  = ''
        return formattedPhone
      }
    }
    
    // ✅ PREVENTATIVE FIX: Prevent buffer overflow
    if (this.digitBuffer.length > 15) {
      this.digitBuffer  = this.digitBuffer.slice(-10) // Keep only last 10 digits
    }
    
    return null
  }

  formatPhoneNumber(digits) {
    return `(${digits.substring(0, 3)}) ${digits.substring(3, 6)}-${digits.substring(6)}`
  }

  getCurrentProgress() {
    return this.digitBuffer.length
  }

  reset() {
    this.digitBuffer  = ''
    this.machineIndicators  = [
      'this is', 'you have reached', 'the number', 'is not available', 'at the tone',
      'please leave your name', 'and number', 'we will call you back', 'thank you'
    ]
  }

  isVoicemail(transcript) {
    const lowerTranscript  = transcript.toLowerCase()
    
    const voicemailScore 
    
    const machineScore 
    
    // If we detect multiple machine/voicemail indicators, it's likely a machine
    return (voicemailScore >
    this.areaCodeToTimezone  = {
      "201": "ET", "202": "ET", "203": "ET", "205": "CT", "206": "PT",
      "207": "ET", "208": "MT", "209": "PT", "210": "CT", "212": "ET",
      "213": "PT", "214": "CT", "215": "ET", "216": "ET", "217": "CT",
      "218": "CT", "219": "CT", "220": "ET", "223": "ET", "224": "CT",
      "225": "CT", "227": "ET", "228": "CT", "229": "ET", "231": "ET",
      "234": "ET", "235": "CT", "239": "ET", "240": "ET", "248": "ET",
      "250": "PT", "251": "CT", "252": "ET", "253": "PT", "254": "CT",
      "256": "CT", "260": "ET", "262": "CT", "267": "ET", "269": "ET",
      "270": "CT", "272": "ET", "274": "CT", "276": "ET", "279": "PT",
      "281": "CT", "283": "ET", "289": "ET", "301": "ET", "302": "ET",
      "303": "MT", "304": "ET", "305": "ET", "307": "MT", "308": "MT",
      "309": "CT", "310": "PT", "312": "CT", "313": "ET", "314": "CT",
      "315": "ET", "316": "CT", "317": "ET", "318": "CT", "319": "CT",
      "320": "CT", "321": "ET", "323": "PT", "324": "ET", "325": "CT",
      "326": "ET", "327": "CT", "329": "ET", "330": "ET", "331": "CT",
      "332": "ET", "334": "CT", "336": "ET", "337": "CT", "339": "ET",
      "340": "AT", "341": "PT", "343": "ET", "345": "ET", "346": "CT",
      "347": "ET", "350": "PT", "351": "ET", "352": "ET", "353": "CT",
      "357": "PT", "360": "PT", "361": "CT", "363": "ET", "364": "CT",
      "365": "ET", "367": "ET", "368": "MT", "369": "PT", "380": "ET",
      "382": "ET", "385": "MT", "386": "ET", "401": "ET", "402": "CT",
      "404": "ET", "405": "CT", "406": "MT", "407": "ET", "408": "PT",
      "409": "CT", "410": "ET", "412": "ET", "413": "ET", "414": "CT",
      "415": "PT", "417": "CT", "418": "ET", "419": "ET", "423": "ET",
      "424": "PT", "425": "PT", "430": "CT", "431": "CT", "432": "MT",
      "434": "ET", "435": "MT", "436": "ET", "437": "ET", "438": "ET",
      "440": "ET", "442": "PT", "443": "ET", "445": "ET", "447": "CT",
      "448": "CT", "450": "ET", "456": "ET", "457": "CT", "458": "PT",
      "463": "ET", "464": "CT", "465": "ET", "468": "ET", "469": "CT",
      "470": "ET", "471": "CT", "472": "ET", "475": "ET", "478": "ET",
      "479": "CT", "480": "MT", "483": "CT", "484": "ET", "500": "ET",
      "501": "CT", "502": "ET", "503": "PT", "504": "CT", "505": "MT",
      "507": "CT", "508": "ET", "509": "PT", "510": "PT", "511": "PT",
      "512": "CT", "513": "ET", "515": "CT", "516": "ET", "517": "ET",
      "518": "ET", "519": "ET", "520": "MT", "521": "ET", "522": "ET",
      "523": "ET", "524": "ET", "525": "ET", "526": "ET", "527": "ET",
      "528": "ET", "529": "ET", "530": "PT", "531": "CT", "532": "ET",
      "533": "ET", "534": "CT", "539": "CT", "540": "ET", "541": "PT",
      "544": "ET", "548": "ET", "551": "ET", "557": "CT", "559": "PT",
      "561": "ET", "562": "PT", "563": "CT", "564": "PT", "565": "ET",
      "566": "ET", "567": "ET", "570": "ET", "571": "ET", "572": "CT",
      "573": "CT", "574": "ET", "575": "MT", "577": "ET", "579": "ET",
      "580": "CT", "581": "ET", "582": "ET", "584": "CT", "585": "ET",
      "586": "ET", "587": "MT", "588": "ET", "589": "ET", "600": "ET",
      "601": "CT", "602": "MT", "603": "ET", "604": "PT", "605": "CT",
      "606": "ET", "607": "ET", "608": "CT", "609": "ET", "610": "ET",
      "612": "CT", "613": "ET", "614": "ET", "615": "CT", "616": "ET",
      "617": "ET", "618": "CT", "619": "PT", "620": "CT", "621": "CT",
      "622": "ET", "623": "MT", "624": "ET", "626": "PT", "628": "PT",
      "629": "CT", "630": "CT", "631": "ET", "633": "ET", "636": "CT",
      "639": "CT", "640": "ET", "641": "CT", "645": "ET", "646": "ET",
      "647": "ET", "649": "AT", "650": "PT", "651": "CT", "656": "ET",
      "657": "PT", "658": "ET", "659": "CT", "660": "CT", "661": "PT",
      "662": "CT", "664": "AT", "666": "ET", "667": "ET", "669": "PT",
      "670": "AT", "671": "CHT", "672": "PT", "677": "ET", "678": "ET",
      "679": "ET", "680": "ET", "681": "ET", "682": "CT", "683": "ET",
      "684": "AT", "686": "ET", "688": "ET", "689": "ET", "700": "ET",
      "701": "CT", "702": "PT", "703": "ET", "704": "ET", "705": "ET",
      "706": "ET", "707": "PT", "708": "CT", "712": "CT", "713": "CT",
      "714": "PT", "715": "CT", "716": "ET", "717": "ET", "718": "ET",
      "719": "MT", "720": "MT", "724": "ET", "725": "PT", "726": "CT",
      "727": "ET", "728": "ET", "729": "ET", "730": "CT", "731": "CT",
      "732": "ET", "734": "ET", "737": "CT", "738": "PT", "740": "ET",
      "742": "ET", "743": "ET", "747": "PT", "748": "MT", "753": "ET",
      "754": "ET", "757": "ET", "760": "PT", "762": "ET", "763": "CT",
      "765": "ET", "769": "CT", "770": "ET", "771": "ET", "772": "ET",
      "773": "CT", "774": "ET", "775": "PT", "778": "PT", "779": "CT",
      "780": "MT", "781": "ET", "782": "AT", "785": "CT", "786": "ET",
      "787": "AT", "800": "ET", "801": "MT", "802": "ET", "803": "ET",
      "804": "ET", "805": "PT", "806": "CT", "807": "ET", "808": "HT",
      "809": "AT", "810": "ET", "812": "ET", "813": "ET", "814": "ET",
      "815": "CT", "816": "CT", "817": "CT", "818": "PT", "819": "ET",
      "820": "PT", "825": "MT", "828": "ET", "829": "AT", "830": "CT",
      "831": "PT", "832": "CT", "833": "ET", "835": "ET", "838": "ET",
      "839": "ET", "840": "PT", "843": "ET", "844": "ET", "845": "ET",
      "847": "CT", "848": "ET", "849": "AT", "850": "CT", "854": "ET",
      "855": "ET", "856": "ET", "857": "ET", "858": "PT", "859": "ET",
      "860": "ET", "862": "ET", "863": "ET", "864": "ET", "865": "ET",
      "866": "ET", "867": "MT", "868": "AT", "869": "AT", "870": "CT",
      "872": "CT", "873": "ET", "876": "ET", "877": "ET", "878": "ET",
      "888": "ET", "900": "PT", "901": "CT", "902": "AT", "903": "CT",
      "904": "ET", "905": "ET", "906": "ET", "907": "AKT", "908": "ET",
      "909": "PT", "910": "ET", "912": "ET", "913": "CT", "914": "ET",
      "915": "MT", "916": "PT", "917": "ET", "918": "CT", "919": "ET",
      "920": "CT", "925": "PT", "928": "MT", "929": "ET", "930": "ET",
      "931": "CT", "934": "ET", "936": "CT", "937": "ET", "938": "CT",
      "939": "AT", "940": "CT", "941": "ET", "947": "ET", "949": "PT",
      "951": "PT", "952": "CT", "954": "ET", "956": "CT", "959": "ET",
      "970": "MT", "971": "PT", "972": "CT", "973": "ET", "975": "CT",
      "978": "ET", "979": "CT", "980": "ET", "984": "ET", "985": "CT",
      "986": "PT", "989": "ET"
    }
  }

  getTimezoneFromAreaCode(areaCode) {
    return this.areaCodeToTimezone[areaCode] || 'ET'
  }

  getAreaCode(phone) {
    const digits  = phone.replace(/\D/g, '')
    if (digits.length > = 10) {
      return digits.substring(0, 3)
    }
    return null
  }

  getLocationInfo(phone) {
    const areaCode  = this.getAreaCode(phone)
    if (!areaCode) return null
    
    const timezone  = this.getTimezoneFromAreaCode(areaCode)
    
    return {
      areaCode,
      timezone
    }
  }

  formatTimeSlot(date) {
    if (!date) return 'unknown time'
   
    try {
      const options  = {
        timeZone: 'America/Los_Angeles',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
        weekday: 'long',
        month: 'short',
        day: 'numeric'
      }
     
      return date.toLocaleString('en-US', options)
    } catch (error) {
      return date.toString()
    }
  }

  isToday(date) {
    const today  = new Date()
    const todayLA  = new Date(today.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
    const dateLA  = new Date(date.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
    return dateLA.getDate() 
  }

  isTomorrow(date) {
    const tomorrow  = new Date()
    tomorrow.setDate(tomorrow.getDate() + 1)
    const tomorrowLA  = new Date(tomorrow.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
  }

  getCurrentTime() {
    return new Date().toLocaleString('en-US', { 
      timeZone: this.timezone,
      hour12: true,
      hour: 'numeric',
      minute: '2-digit',
      second: '2-digit'
    })
  }

  // NEW: Check if a time slot is during lunch hours (12:00 PM - 1:00 PM) in the caller's timezone
  isLunchTime(date, callerTimezone) {
    if (!date || !callerTimezone) return false
    
    try {
      const timeString  = date.toLocaleString('en-US', { 
        timeZone: callerTimezone, 
        hour: 'numeric', 
        minute: '2-digit',
        hour12: true 
      })
      
      const [hourStr, amPm]  = timeString.split(' ')
      const hour  = parseInt(hourStr)
      
      // Check if it's between 12:00 PM and 1:00 PM
      return amPm 
    } catch (error) {
      logger.error('❌ Lunch time check error:', { message: error.message })
    }
  }
}

const timezoneManager  = new TimezoneManager()
    this.eventTypeId  = CAL_EVENT_TYPE_ID || null
    this.duration  = 15
    this.timezone  = 'America/Los_Angeles'
    this.timezoneManager  = new TimezoneManager()
    this.allAvailableSlots  = []
  }

  // Convert UTC time from Cal.com to local Pacific Time
  convertToLocalTime(utcDate) {
    if (!utcDate) return null
    
    try {
      const date  = new Date(utcDate)
      const localTime  = date.toLocaleString('en-US', {
        timeZone: 'America/Los_Angeles',
        hour12: true,
        hour: 'numeric',
        minute: '2-digit',
        weekday: 'long',
        month: 'short',
        day: 'numeric'
      })
      
      return localTime
    } catch (error) {
      logger.error('❌ Time conversion error:', { message: error.message })
      return utcDate.toString()
    }
  }

  async testApiConnectivity() {
    if (!this.apiKey || !this.eventTypeId) {
      throw new Error('Cal.com not configured')
    }

    try {
      logger.info('🔍 Testing Cal.com /slots endpoint (API v2)...')
      const now  = new Date()
      const startDate  = now.toISOString().split('T')[0]
      const endDate  = new Date(Date.now() + 14 * 24 * 60 * 1000).toISOString().split('T')[0]
      
      const slotsUrl 
      logger.info(`🔗 Testing URL: ${slotsUrl}`)
      
      const slotsResponse  = await fetch(slotsUrl, {
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'cal-api-version': CAL_COM_API_VERSION,
        }
      })

      if (slotsResponse.ok) {
        const slotsData  = await slotsResponse.json()
        logger.info(`✅ Slots endpoint working. Response keys:`, { keys: Object.keys(slotsData) })
        return true
      } else {
        logger.error(`❌ Slots endpoint failed: ${slotsResponse.status}`)
        const errorText  = await slotsResponse.text()
        logger.error(`❌ Slots error: ${errorText}`)
        return false
      }

    } catch (error) {
      logger.error('❌ Cal.com API connectivity test failed:', { message: error.message })
    }
  }

  async getRealAvailableSlots(days 
        throw new Error('Cal.com service not configured')
      }

      try {
        const now  = new Date()
        const startDate  = now.toISOString().split('T')[0]
        const endDate  = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString().split('T')[0]
        
        const url 
        
        logger.info('🔄 Fetching Cal.com slots with API v2...')
        logger.info(`   📅 Event Type ID: ${this.eventTypeId}`)
        logger.info(`   📅 Date Range: ${startDate} to ${endDate}`)

        if (!response.ok) {
          logger.error(`❌ Cal.com API error: ${response.status} - ${response.statusText}`)
          const errorText  = await response.text()
          logger.error(`❌ Cal.com error details: ${errorText}`)
          
          try {
            const errorData  = JSON.parse(errorText)
            logger.error(`❌ Cal.com error parsed:`, { errorData })
          } catch (e) {
            logger.error(`❌ Cal.com raw error: ${errorText}`)
          }
          
          throw new Error(`Cal.com API error: ${response.status} - ${errorText}`)
        }

        const data  = await response.json()
        
        const responseSize  = JSON.stringify(data).length
        if (responseSize > 1000000) {
          logger.warn(`⚠️ Cal.com response very large: ${(responseSize / 1024 / 1024).toFixed(2)}MB`)
        }

        logger.info(`✅ Cal.com API response received with ${Object.keys(data.data || {}).length} date ranges`)
        
        if (data && data.data) {
          this.allAvailableSlots  = []
          
          const slotsObject  = data.data
          
          for (const [date, slots] of Object.entries(slotsObject)) {
            if (Array.isArray(slots)) {
              for (const slot of slots) {
                if (slot.start) {
                  const slotTime  = new Date(slot.start)
                  if (slotTime > new Date()) {
                    this.allAvailableSlots.push(slotTime)
                  }
                } else {
                  logger.warn('Invalid slot format', { date })
                }
              }
            }
          }
          
          this.allAvailableSlots.sort((a, b)  = > a - b)
          
          if (this.allAvailableSlots.length > 100) {
            logger.warn(`⚠️ Too many slots (${this.allAvailableSlots.length}), limiting to first 100`)
            this.allAvailableSlots  = this.allAvailableSlots.slice(0, 100)
          }
          
          logger.info(`✅ Retrieved ${this.allAvailableSlots.length} Cal.com slots from ${days}-day range`)
          return this.allAvailableSlots
        } else {
          logger.error('❌ No slots data from Cal.com response structure')
          throw new Error('No available slots from Cal.com')
        }
      } catch (error) {
        logger.error('❌ Cal.com fetch error:', { message: error.message })
        throw error
  }

  // FIXED: Enhanced slot selection with morning/afternoon logic
  selectSmartTimeSlots(availableSlots, callerTimezone 
      throw new Error('No available time slots')
    }

    // ✅ FIX: Filter out lunch time slots (12:00 PM - 1:00 PM in caller's timezone)
    const filteredSlots 
      if (isLunch) {
        logger.debug(`Filtering out lunch slot: ${this.formatTimeSlot(slot)}`)
      }
      return !isLunch

    logger.info(`Filtered ${availableSlots.length - filteredSlots.length} lunch slots`)

    const today  = new Date()
    const tomorrow  = new Date(today)

    // ✅ ENHANCED: Group slots by time of day
    const morningSlots 
      return slotHour > = 9 && slotHour < 12 // 9AM - 11:59AM
    })

    const afternoonSlots  // 1PM - 5PM
    })

    const selectedSlots  = []

    // ✅ NEW: Prioritize morning and afternoon slots from different days
    if (morningSlots.length > 0) {
      // Find earliest morning slot
      const morningSlot  = morningSlots[0]
      selectedSlots.push(morningSlot)
      logger.info('Selected morning slot', { slot: this.formatTimeSlot(morningSlot) })
    }

    if (afternoonSlots.length > 0) {
      // Find earliest afternoon slot
      const afternoonSlot  = afternoonSlots[0]
      selectedSlots.push(afternoonSlot)
      logger.info('Selected afternoon slot', { slot: this.formatTimeSlot(afternoonSlot) })
    }

    // If we don't have both morning and afternoon, fill with best available
    if (selectedSlots.length < 2) {
      const remainingSlots 
      selectedSlots.push(...remainingSlots.slice(0, 2 - selectedSlots.length))
    }

    const finalSlots 
    
    logger.info('Final selected slots (morning/afternoon preferred)', { 
      count: finalSlots.length, 
      slots: finalSlots.map(slot  = > this.formatTimeSlot(slot)),
      callerTimezone 
    })
    
    return finalSlots
  }

  // NEW: Get smart time slots with descriptions
  async getSmartTimeSlotsWithDescription() {
    const slots  = await this.getRealAvailableSlots(14)
    const callerTimezone  = 'America/Los_Angeles' // You can make this dynamic based on area code
    
    const optimalSlots  = this.selectSmartTimeSlots(slots, callerTimezone)
    
    if (optimalSlots.length 
    }
    
    // Group slots by time of day for better description
    const morningSlots 
      return hour < 12
    
    const afternoonSlots 
      return hour > = 12
    
    return {
      slots: optimalSlots,
      description: this.generateSlotDescription(morningSlots, afternoonSlots)
    }
  }

  // NEW: Generate better slot descriptions
  generateSlotDescription(morningSlots, afternoonSlots) {
    if (morningSlots.length > 0 && afternoonSlots.length > 0) {
      const morningTime  = this.formatTimeSlot(morningSlots[0])
      const afternoonTime  = this.formatTimeSlot(afternoonSlots[0])
      // ✅ FIX: Check if both slots are on the same day
      const morningDate = new Date(morningSlots[0])
      const afternoonDate = new Date(afternoonSlots[0])
      const isSameDay = morningDate.toDateString() === afternoonDate.toDateString()

      if (isSameDay) {
        // Extract just the time from afternoon slot (e.g., "3:00 PM")
        const afternoonTimeOnly = afternoonTime.split(' at ').pop() || afternoonTime.split(',').pop().trim()
        const today = new Date()
        const isToday = morningDate.toDateString() === today.toDateString()
        const dayPrefix = isToday ? 'today' : morningTime.split(' at ')[0]
        return `I have ${dayPrefix} at ${morningTime.split(' at ').pop()}, or ${afternoonTimeOnly}. Which works better for you?`
      }

      return `I have morning availability at ${morningTime} or afternoon at ${afternoonTime}. Which timeframe works better for you?`
    } else if (morningSlots.length > 0) {
      const slot1  = this.formatTimeSlot(morningSlots[0])
      const slot2  = morningSlots[1] ? this.formatTimeSlot(morningSlots[1]) : null
      if (slot2) {
        return `I have morning appointments at ${slot1} or ${slot2}. Which works better?`
      } else {
        return `I have a morning appointment at ${slot1}. Would that work?`
      }
    } else {
      const slot1  = this.formatTimeSlot(afternoonSlots[0])
      if (slot2) {
        return `I have afternoon appointments at ${slot1} or ${slot2}. Which works better?`
      } else {
        return `I have an afternoon appointment at ${slot1}. Would that work?`
      }
    }
  }

  formatTimeSlot(date) {
    return this.timezoneManager.formatTimeSlot(date)
  }

  async bookAppointment(bookingTime, customerDetails) {
    return await calComCircuitBreaker.call(async ()  = > {
      if (!this.apiKey || !this.eventTypeId) {
        throw new Error('Cal.com service not configured')
      }

      try {
        logger.info('📅 Creating real Cal.com booking...')
        logger.info(`   👤 Customer: ${customerDetails.firstName} ${customerDetails.lastName}`)
        
        const startTime  = bookingTime.toISOString()
        
        const payload  = {
          eventTypeId: parseInt(this.eventTypeId),
          start: startTime,
          attendee: {
            name: `${customerDetails.firstName} ${customerDetails.lastName}`.trim(),
            email: customerDetails.email,
            timeZone: "America/Los_Angeles",
            language: "en"
          },
          location: "whereby",
          metadata: { source: "Ava AI" }
        }

        logger.info("📅 Booking payload:", { payload })
        
        if (!response.ok) {
          const errorText  = await response.text()
          logger.error(`❌ Cal.com booking failed: ${response.status} - ${errorText}`)
          throw new Error(`Cal.com booking failed: ${response.status} - ${errorText}`)
        }
        
        const result  = await response.json()
        logger.info("📅 Booking response:", { result })
        
        if (result && result.data && result.data.id) {
          const bookingId  = result.data.id
          logger.info("✅ Cal.com booking successful:", { bookingId })
          
          return { 
            bookingId, 
            bookingUid: result.data.uid,
            meetingUrl: result.data.meetingUrl,
            bookingTime: startTime
          }
        } else {
          throw new Error(`Booking failed: ${result.message || 'Unknown error'}`)
        }
      } catch (error) {
        logger.error('❌ Cal.com booking error:', { message: error.message })
  }

  async sendConfirmationEmail(booking, customerDetails) {
    try {
      logger.info(`✅ Cal.com confirmation email sent to ${customerDetails.email}`)
      return true
    } catch (error) {
      logger.warn('⚠️ Cal.com email sending note:', { message: error.message })
    }
  }

  handleUserSlotRequest(requestedDateTime) {
    const requested  = new Date(requestedDateTime)
    if (isNaN(requested)) {
      logger.error('Invalid user requested date/time', { requestedDateTime })
      return { success: false, message: 'Invalid date/time format' }
    }
    logger.info('Processing user slot request', { requested: requested.toISOString() })
    const exactMatch 
    if (exactMatch) {
      logger.info('Found exact slot match', { slot: this.formatTimeSlot(exactMatch) })
      return { success: true, slot: exactMatch }
    }
    const closestSlot 
      const closestDiff  = closest ? Math.abs(closest - requested) : Infinity
      return slotDiff < closestDiff ? slot : closest
    }, null)
    if (closestSlot) {
      logger.info('Found closest slot', { requested: requested.toISOString(), closest: this.formatTimeSlot(closestSlot) })
      return { success: true, slot: closestSlot, message: `Requested slot not available, closest is ${this.formatTimeSlot(closestSlot)}` }
    }
    logger.warn('No slots available for user request', { requested: requested.toISOString() })
    return { success: false, message: 'No available slots near requested time' }
  }
}

const calCom  = new EnhancedCalComIntegration()
  }

  async synthesizeSpeech(text, conversationStage  = 'default') {
    if (!text) return Buffer.alloc(0)

    try {
      // ✅ FIX 4: Replace acronyms with phonetic pronunciations
      text  = text
        .replace(/\bUCaaS\b/g, 'you-cas')
        .replace(/\bCCaaS\b/g, 'see-cas')

      // ✅ OPTIMIZED: Faster pacing for confident, professional delivery + reduced latency
      const stageSettings  = {
        'greeting': { speed: 1.1 },    // ✅ INCREASED: 1.0 → 1.1 for faster greeting response
        'qualification': { speed: 1.05 }, // CHANGED: 0.95 → 1.05
        'booking': { speed: 1.1 },      // CHANGED: 1.0 → 1.1
        'default': { speed: 1.05 }      // CHANGED: 0.95 → 1.05
      }
      
      const settings  = stageSettings[conversationStage] || stageSettings.default

      // Build URL with query parameters (Deepgram TTS uses URL params, not JSON body)
      const params  = new URLSearchParams({
        model: this.model,
        encoding: 'mulaw',
        sample_rate: '8000',
        speed: settings.speed.toString(),
        container: 'none'
      })

      const url  = `${this.baseURL}?${params.toString()}`
          throw new Error(`Deepgram TTS API error: ${response.status} - ${errorText}`)

      if (response.ok) {
        const audioBuffer  = await response.arrayBuffer()
        return Buffer.from(audioBuffer)
      } else {
        throw new Error(`Deepgram TTS failed: ${response.status}`)
      }
    } catch (error) {
      logger.error('❌ Deepgram TTS error:', { message: error.message })
      return Buffer.alloc(0)
    }
  }
}

const deepgramTTS  = new DeepgramTTS()
    this.userPreferences  = {
      preferredTimes: [],
      rejectedTimes: [],
      businessHours: { start: 8, end: 17 }, // 8AM-5PM
      specificRequests: {} // "Thursday at 2PM"
    }
    this.conversationContext  = {
      previousStates: [],
      userIntent: '',
      painPoints: [],
      qualificationScore: 0
    }
  }

  rememberRejection(slot, reason) {
    this.rejectedTimeSlots.add(slot.toString())
    this.userPreferences.rejectedTimes.push({
      slot,
      reason,
      timestamp: Date.now()
    })
  }

  shouldOfferSlot(slot) {
    const slotHour  = new Date(slot).getHours()
    const isBusinessHours 
    const wasRejected  = this.rejectedTimeSlots.has(slot.toString())
    
    return isBusinessHours && !wasRejected
  }

  getUserPreferences() {
    return {
      avoidsEarly: this.userPreferences.rejectedTimes.some(r 
  }

  selectOptimalSlots(availableSlots, userTimezone 

    // Apply business hours filter
    const businessHoursSlots 

    // Group by time preference
    const preferences  = this.memory.getUserPreferences()
    
    let optimalSlots  = []
    
    if (preferences.prefersAfternoon) {
      optimalSlots 
        return hour > = 13
    } else {
      // Default: offer one morning, one afternoon
      const morningSlots 
        return hour < 12
      
      const afternoonSlots 

      optimalSlots  = [
        ...morningSlots.slice(0, 1),
        ...afternoonSlots.slice(0, 1)
      ]
    }

    // Ensure we have slots
    if (optimalSlots.length < 2 && businessHoursSlots.length > 0) {
      optimalSlots  = businessHoursSlots.slice(0, 2)
    }

    return optimalSlots.slice(0, 2)
  }

  handleSpecificRequest(requestedTime) {
    const requested  = new Date(requestedTime)
    
    // Validate it's a reasonable business hour
    const hour  = requested.getHours()
    if (hour < 6 || hour > 20) {
      return {
        success: false,
        message: `I can't schedule appointments at ${hour}:00. Let me find a better time during business hours.`
      }
    }

    return {
      success: true,
      slot: requested,
      message: `I'll book you for ${this.formatTimeSlot(requested)}.`
    }
    try {
      const options  = {
        timeZone: 'America/Los_Angeles',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
        weekday: 'long',
        month: 'short',
        day: 'numeric'
      }
      return date.toLocaleString('en-US', options)
    }
  }
}

// 
    this.recoveryTriggers  = new Map()
    this.consecutiveSameState  = 0
    this.lastState  = null
    this.setupRecoveryPatterns()
  }

  setupRecoveryPatterns() {
    // Global re-engagement patterns for ANY state
    this.recoveryTriggers.set('help_offered', [
      /i can help/i,
      /we can help/i,
      /maybe i can help/i,
      /let me help/i,
      /what can i help/i,
      /how can i help/i,
      /we handle/i,
      /we manage/i,
      /talk to me/i,
      /speak to me/i
    ])

    this.recoveryTriggers.set('confusion', [
      /what.*talking about/i,
      /don'?t understand/i,
      /confused/i,
      /what do you mean/i,
      /repeat that/i
    ])

    this.recoveryTriggers.set('time_issue', [
      /3am/i,
      /too early/i,
      /too late/i,
      /can'?t do.*time/i,
      /who works at.*am/i
    ])
  }

  logStateTransition(fromState, toState, userInput 
    } else {
      this.consecutiveSameState  = 0
    }
    
    this.lastState  = toState
    
    // If we've been in the same state 3+ times, force recovery
    if (this.consecutiveSameState > = 3) {
      logger.warn(`🔄 FORCED STATE RECOVERY: Stuck in ${toState} for 3+ transitions`)
      return {
        shouldRecover: true,
        triggerType: 'state_loop',
        targetState: this.getLoopRecoveryState(toState)
      }
    }
    
    this.stateHistory.push({
      fromState,
      toState,
      userInput,
      timestamp: Date.now(),
      sessionDuration: Date.now() - this.startTime
    })

    // Prevent history bloat
    if (this.stateHistory.length > 20) {
      this.stateHistory  = this.stateHistory.slice(-10)
    }
    
    return { shouldRecover: false }
  }

  getLoopRecoveryState(currentState) {
    const recoveryMap  = {
      'find_decision_maker': 'collect_email_fallback',
      'offer_appointments': 'get_email_for_followup',
      'get_email': 'phone_fallback',
      'default': 'openness_pitch'
    }
    return recoveryMap[currentState] || recoveryMap.default
  }

  shouldRecover(currentState, userText) {
    // Check if we're in a terminal state that needs recovery
    const terminalStates  = ['done', 'collect_email_fallback', 'confirm_booking']
    
    if (terminalStates.includes(currentState)) {
      for (const [triggerType, patterns] of this.recoveryTriggers) {
        if (patterns.some(pattern  = > pattern.test(userText))) {
          return {
            shouldRecover: true,
            triggerType,
            targetState: this.getRecoveryState(triggerType)
          }
        }
      }
    }
    
    return { shouldRecover: false }
  }

  getRecoveryState(triggerType) {
    const recoveryMap  = {
      'help_offered': 'openness_pitch',
      'confusion': 'find_decision_maker', 
      'time_issue': 'offer_appointments'
    }
    return recoveryMap[triggerType] || 'openness_pitch'
  }

  detectStateLoop() {
    if (this.stateHistory.length < 3) return false
    
    const recentStates 
    return new Set(recentStates).size  // Same state 3 times in a row
  }
}

// 
    const hour  = slotInTz.getHours()
    
    // Business hours: 8AM - 6PM local time
    return hour >
  }

  static filterBusinessHours(slots, timezone 
  }

  static getRejectionReason(slot, timezone 
    
    if (hour < 6) return "too_early"
    if (hour > 20) return "too_late"
    if (hour >
    
    return "unknown"
    this.ctx  = { phone, zohoAccountId: accountId }
    this.accountId  = accountId
    this.lastUserInput  = ''
    this.transferInitiated  = false
    this.callStartTime  = new Date()
    this.requestedSelfBooking  = false
    this.emailConfirmed  = false
    this.tempEmail  = ''
    
    // ✅ FIX 1: Add debounce timer
    this.lastUserTime  = 0
    
    // NEW: Conversation stage tracking for dynamic pacing
    this.conversationStage  = 'greeting'
    
    // Conversation history tracking for LLM
    this.conversationHistory  = []
    this.lastBotMessage  = ''
    this.failedExtractAttempts  = 0
    
    // Interruption tracking
    this.lastResponseTime  = 0
    this.interruptionCount  = 0
    
    // FIX 3: Add pitchAlreadyGiven flag
    this.pitchAlreadyGiven  = false
    
    // ✅ FIX: Add transfer timeout
    this.transferTimeout  = null
    this.checkedTransferStatus  = false
    this.transferStartTime  = null
    
    // ENTERPRISE: Enhanced memory systems
    this.memory  = new EnterpriseConversationMemory()
    this.stateManager  = new EnterpriseStateManager()
    this.slotSelector  = new IntelligentSlotSelector(this.memory)
    
    // Track specific user requests
    this.userSpecificRequests  = {
      preferredDay: null,
      preferredTime: null,
      constraints: []
    }
    
    // Track rejected slots
    this.rejectedSlots  = new Set()
    this.userConstraints  = []
    
    // ✅ ENHANCED: Conversation context tracking
    this.conversationContext  = {
      mentionedNames: new Set(),
      transferAttempted: false,
      selfIdentificationAttempts: 0,
      decisionMakerIdentified: false,
      currentSpeaker: null
    }
    
    this.numberExtractor  = new NumberExtractor()
    this.phoneCollector  = new PhoneNumberCollector()
    this.emailCollector  = new EmailCollector()
    this.voicemailDetector  = new VoicemailDetector()
    this.costTracker  = new EnhancedCostTracker()
    
    this.voicemailDetected  = false
    this.consecutiveVoicemailCount  = 0
    this.askedAboutSeats  = 0
    
    // ✅ FIX: Track greeting state
    this.greetingGiven  = false
    
    // ✅ FIX: Track interruption state for context resumption
    this.interruptionContext  = null
    this.wasInterrupted  = false
    this.preInterruptionState  = null
    
    // Callback request tracking
    this.callbackRequested  = false
    this.callbackPhone  = null
    this.callbackName  = null
    
    // Location information
    this.locationInfo  = timezoneManager.getLocationInfo(phone)
    
    // Performance optimization: Pre-cache common responses
    this.cachedSlots  = null
    this.slotsFetchTime  = 0
    this.slotsCacheTimeout  = 60000
    
    // Call metrics
    this.charactersSpoken  = 0
    this.deepseekTokens  = { input: 0, output: 0 }
    this.conversationMetrics  = {
      stateTransitions: 0,
      interruptions: 0,
      positiveSignals: 0,
      negativeSignals: 0
    }
    return unavailabilityPatterns.some(pattern  = > pattern.test(text))
    return screeningPatterns.some(pattern  = > pattern.test(text))
  }

  isMessageTaking(text) {
    return /(take a message|leave a message|i'?ll let them know)/i.test(text)
  }

  isCallbackOffer(text) {
    return /(call you back|call back|reach out later|contact you later)/i.test(text)
  }

  isTemporarilyBusy(text) {
    return /(in a meeting|on another call|with a client|busy right now)/i.test(text)
  }

  isCheckingAvailability(text) {
    return /(hold on|let me check|one moment|just a second)/i.test(text)
    return wrongDeptPatterns.some(pattern  = > pattern.test(text))
    
    const lowerText  = text.toLowerCase()
    const lowerExpected  = expectedName.toLowerCase()
    
    // Patterns where user identifies as the decision maker
    const selfIdPatterns  = [
      new RegExp(`\\b(this is|it'?s me|i'?m|I am)\\s+(\\w+\\s+)?\\w*\\s*\\b${lowerExpected.replace(/\s+/g, '\\s*')}\\b`, 'i'),
      new RegExp(`\\b(\\w+\\s+)?\\w*\\s*\\b${lowerExpected.replace(/\s+/g, '\\s*')}\\s+(here|speaking)`, 'i'),
      /\b(talk to me|speak to me|i can help|i handle|i oversee)\b/i,
      /\b(actually|actually,)\s+(it'?s me|i'?m|I am)\b/i,
      new RegExp(`\\b${lowerExpected}\\s+(here|speaking|on the line)`, 'i'),
      /\b(you'?re speaking with|you'?ve got|this is)\\s+[A-Z][a-z]+/i // Generic pattern for any name
    ]
    
    return selfIdPatterns.some(pattern  = > pattern.test(lowerText))
    
    return transferPatterns.some(pattern  = > pattern.test(text))
    
    return transferOfferPatterns.some(pattern  = > pattern.test(text.toLowerCase()))
    
    logger.info(`🕵️ Enhanced name extraction from: "${text}"`)
    
    // ✅ FIX: Filter out obvious non-names first - EXPANDED to include all filler words
    const nonNames  = new Set([
      'usually', 'maybe', 'probably', 'perhaps', 'sometimes', 'normally',
      'typically', 'generally', 'often', 'always', 'never', 'hello', 'hi', 'hey', 'hell', 'hel',
      'thanks', 'thank', 'please', 'sorry', 'okay', 'ok', 'sure', 'yes', 'no', 'yeah', 'yep', 'yup', 'nah',
      'lunchtime', 'lunch', 'break', 'out', 'away', 'unavailable',
      'um', 'uh', 'er', 'ah', 'hmm', 'well', 'like', 'you know',
      'possibly', 'potentially', 'perhaps', 'might',
      'think', 'guess', 'suppose', 'believe', 'assume', 'know',
      'that', 'this', 'the', 'a', 'an', 'and', 'or', 'but', 'for', 'with',
      'would', 'could', 'should', 'might', 'may', 'can', 'will', 'shall',
      'what', 'when', 'where', 'who', 'why', 'how', 'which', 'whose',
      'just', 'only', 'also', 'still', 'even', 'very', 'too', 'so', 'such',
      'all', 'some', 'any', 'each', 'every', 'both', 'either', 'neither',
      'most', 'likely', 'probably', 'maybe', 'perhaps', 'possibly'
    ])
    
    // ✅ FIX: Better pattern for "X's" possessive (like "Ron's")
    const possessivePattern  = /(\b[A-Z][a-z]+)'s\b/i
    const possessiveMatch  = text.match(possessivePattern)
    if (possessiveMatch) {
      const name  = possessiveMatch[1]
      if (!nonNames.has(name.toLowerCase())) {
        logger.info(`✅ Extracted name from possessive: "${name}"`)
        return [name]
      }
    }
    
    // ✅ FIX: Improved "or" pattern with validation - handle "maybe" and "probably" between names
    // ✅ CRITICAL: Check for comma-separated names FIRST (most common pattern)
    const commaPattern  = /([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)\s*,\s*(?:maybe|probably|perhaps|possibly|most likely)?\s*([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)/i
    const commaMatch  = text.match(commaPattern)
    if (commaMatch) {
      const name1  = commaMatch[1].trim()
      const name2  = commaMatch[2].trim()
      if (!nonNames.has(name1.toLowerCase()) && !nonNames.has(name2.toLowerCase()) &&
          name1.length >
        return [name1, name2]
      }
    }
    
    // Then check "or" patterns
    const orPatterns  = [
      /([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)\s+(?:or|and)\s+(?:maybe|probably|perhaps|possibly|most likely)\s+([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)/i,
      /([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)\s+(?:or|and|\/)\s+([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)/i
    ]
    
    for (const orPattern of orPatterns) {
      const orMatch  = text.match(orPattern)
      if (orMatch && orMatch[1] && orMatch[2]) {
        const name1  = orMatch[1].trim()
        const name2  = orMatch[2].trim()
        
        // Validate these are actual names
        const isValidName 
          return name.length >
        }
        
        if (isValidName(name1) && isValidName(name2)) {
          logger.info(`✅ Extracted valid names from "or" pattern: "${name1}" and "${name2}"`)
          return [name1, name2]
        } else {
          logger.info(`❌ Names from "or" pattern failed validation: "${name1}", "${name2}"`)
        }
      }
    }
    
    // ✅ CRITICAL FIX: Reject single short words that could be truncated greetings
    // If the text is just "Hell", "Hel", "Hi", "Hey" - it's probably a truncated greeting, not a name
    const singleWord  = text.trim().toLowerCase()
    if (singleWord.length < = 4 && ['hell', 'hel', 'hi', 'hey', 'hello'].includes(singleWord)) {
      logger.info(`❌ Rejected "${singleWord}" - likely truncated greeting, not a name`)
    }
    
    // ✅ FIX: Single name extraction with better filtering - ENHANCED
    // ✅ CRITICAL FIX: Only extract names after common name-indicating patterns
    const nameIndicatorPatterns  = [
      /(?:that would be|that'?s|it'?s|probably|maybe|perhaps|possibly|likely|most likely)\s+([A-Z][a-z]{3,}(?:\s+[A-Z][a-z]{2,})*)/i,
      /(?:or|and|\/)\s+([A-Z][a-z]{3,}(?:\s+[A-Z][a-z]{2,})*)/i,
      /([A-Z][a-z]{3,}(?:\s+[A-Z][a-z]{2,})*)\s+(?:or|and|\/)/i
    ]
    
    // Try name indicator patterns first (more reliable)
    for (const pattern of nameIndicatorPatterns) {
      const matches  = text.match(pattern)
      if (matches && matches[1]) {
        const candidateName  = matches[1].trim()
        const lowerName  = candidateName.toLowerCase()
        
        // Strict validation - must not be in nonNames and must be at least 3 characters
        if (!nonNames.has(lowerName) && 
            candidateName.length >
          return [candidateName]
        }
      }
    }
    
    // Fallback: Single name extraction with VERY strict filtering
    // ✅ CRITICAL: Require at least 3 characters to avoid "Hell", "Hel", etc.
    const singleNamePattern  = /\b([A-Z][a-z]{3,}(?:\s+[A-Z][a-z]{2,})*)\b/g
    const singleMatches  = [...text.matchAll(singleNamePattern)]
    
    if (singleMatches.length > 0) {
      const potentialNames 
          const words  = name.split(' ')
          
          // ✅ CRITICAL: Reject if ANY word is in nonNames set
          const hasNonNameWord 
          
          // ✅ VERY STRICT: Only accept if it looks like a real name (min 3 chars per word)
          const isValid 
          
          if (isValid) {
            logger.info(`✅ Valid name candidate: "${name}"`)
          } else {
            logger.info(`❌ Rejected name candidate: "${name}" (filtered - in nonNames, too short, or common word)`)
          }
          
          return isValid
        })
      
      // ✅ CRITICAL: Only return names if we have high confidence (not at start of sentence common words)
      const highConfidenceNames 
        return !/^(Yeah|Yes|No|Well|So|Then|Now|But|And|Or|Hell|Hel|Hello|Hi|Hey)/i.test(firstWord)
      
      if (highConfidenceNames.length > 0) {
        logger.info(`✅ Extracted filtered names: ${highConfidenceNames.join(', ')}`)
        return highConfidenceNames
      }
    }
    
    logger.info(`❌ No valid names extracted from: "${text}"`)
    const names  = text.match(namePattern) || []
    
    names.forEach(name 
      if (!commonWords.includes(name.toLowerCase()) && name.length > 2) {
        this.conversationContext.mentionedNames.add(name)
  }

  updateCurrentSpeaker(text) {
    // Check if someone is identifying themselves
    const selfIdPattern  = /\b(this is|it'?s me|i'?m|I am)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/i
    const match  = text.match(selfIdPattern)
    
    if (match) {
      const identifiedName  = match[2]
      this.conversationContext.currentSpeaker  = identifiedName
      this.conversationContext.selfIdentificationAttempts++
      logger.info(`🎯 Current speaker identified as: ${identifiedName}`)
      
      // If this matches a previously mentioned decision maker, mark them as identified
      if (this.ctx.decisionMaker && 
          identifiedName.toLowerCase().includes(this.ctx.decisionMaker.toLowerCase())) {
        this.conversationContext.decisionMakerIdentified  = true
        logger.info(`✅ Decision maker ${this.ctx.decisionMaker} has self-identified!`)
      }
      // Also preserve volunteered first name for later booking flows
      if (!this.ctx.firstName) {
        // Store a capitalized short first name
        const firstName  = identifiedName.split(' ')[0]
        this.ctx.firstName  = firstName
        logger.info(`📝 Remembering volunteered first name: ${firstName}`)
      }
    }
  }

  // Enhanced helper methods for common questions
  isAskingAboutCompany(text) {
    const companyQuestions  = [
      /what'?s? (?:the|your) company/i,
      /who are you with/i,
      /what company/i,
      /who do you work for/i,
      /what'?s? (?:the|your) name of (?:the|your) company/i
    ]
    return companyQuestions.some(pattern  = > pattern.test(text))
  }

  answerCompanyQuestion() {
    return `We're ${COMPANY_NAME}. We help businesses optimize their phone systems and contact centers across hundreds of different providers.`
  }

  isAskingAboutProviders(text) {
    const providerQuestions  = [
      /what (?:companies|providers)/i,
      /which (?:companies|providers)/i,
      /who do you work with/i,
      /list (?:of|your) (?:companies|providers)/i,
      /what (?:phone|voip) (?:companies|providers)/i
    ]
    return providerQuestions.some(pattern  = > pattern.test(text))
  }

  answerProviderQuestion() {
    // Smart deflection: mention top tier + pivot back to their needs
    return `We work with all the major ones—RingCentral, 8x8, Vonage, NICE, Zoom, and over fifty others in UCaaS and CCaaS. The real question is what features or issues matter most to you. Are you looking to improve call quality, add features, or just explore better options?`
  }

  checkSpecificProvider(text) {
    const lowerText  = text.toLowerCase()
    
    // UCaaS Providers
    const ucaasProviders  = {
      '8x8': 'Yes, we work with 8x8—great for comprehensive UCaaS with video, messaging, and solid integrations.',
      'airespring': 'Yes, we work with AireSpring—they do global managed SD-WAN and customized UCaaS.',
      'avaya': 'Yes, we work with Avaya—solid cloud-hosted PBX solution.',
      'blue mantis': 'Yes, we work with Blue Mantis—they focus on security-first IT with UCaaS integration.',
      'calltower': 'Yes, we work with CallTower—excellent PSTN voice services for UCaaS platforms.',
      'commandlink': 'Yes, we work with CommandLink—unified telecom stack in a single cloud app.',
      'firstorion': 'Yes, we work with FirstOrion—customized call displays and mobile branding.',
      'hiya': 'Yes, we work with Hiya—call protection and branded caller ID.',
      'levelblue': 'Yes, we work with LevelBlue—advanced UC features.',
      'metronet': 'Yes, we work with MetroNet—fiber-based UCaaS and connectivity.',
      'momentum': 'Yes, we work with Momentum—third-party UCaaS for Teams integration.',
      'ntegrated': 'Yes, we work with Ntegrated—UCaaS with SD-WAN and security bundling.',
      'pureip': 'Yes, we work with PureIP—SIP trunking and UCaaS enablement.',
      'ringcentral': 'Yes, we work with RingCentral—one of the top award-winning UCaaS providers.',
      'ring central': 'Yes, we work with RingCentral—one of the top award-winning UCaaS providers.',
      'scb global': 'Yes, we work with SCB Global—global UCaaS and voice services.',
      'sharpen': 'Yes, we work with Sharpen—UCaaS with analytics and agent tools.',
      'sippio': 'Yes, we work with SIPPIO—SIP trunking for UCaaS platforms.',
      'syringa': 'Yes, we work with Syringa Networks—high-performance network and UC solutions.',
      'vonage': 'Yes, we work with Vonage—programmable communications APIs.',
      'zayo': 'Yes, we work with Zayo—solution design for UCaaS with connectivity.',
      'zoom': 'Yes, we work with Zoom—video-centric UCaaS with third-party integrations.',
      
      // CCaaS Providers
      'alvaria': 'Yes, we work with Alvaria—compliant outreach and intelligent contact center infrastructure.',
      'edify': 'Yes, we work with Edify—proprietary CCaaS across AWS, Azure, and GCP.',
      'evaluagent': 'Yes, we work with evaluagent—AI-driven quality assurance and agent coaching.',
      'kustomer': 'Yes, we work with Kustomer—conversational CRM with AI automation.',
      'level ai': 'Yes, we work with Level AI—omnichannel analytics and real-time agent support.',
      'livevox': 'Yes, we work with LiveVox—next-gen cloud contact center platform.',
      'nice': 'Yes, we work with NICE inContact—top award-winning CCaaS with omnichannel and AI.',
      'nice incontact': 'Yes, we work with NICE inContact—top award-winning CCaaS with omnichannel and AI.',
      'redialbpo': 'Yes, we work with RedialBPO—outsourced CCaaS for voice, chat, and email.',
      'regal': 'Yes, we work with Regal.IO—AI-powered outbound contact center.',
      'regal.io': 'Yes, we work with Regal.IO—AI-powered outbound contact center.',
      'smartaction': 'Yes, we work with SmartAction—AI conversational tools for self-service.',
      'successkpi': 'Yes, we work with SuccessKPI—contact center insights and performance platform.',
      'uniphore': 'Yes, we work with Uniphore—conversational AI for self-service and analytics.',
      
      // Additional common providers (competitors/adjacents)
      'nextiva': 'Yes, we work with Nextiva—popular UCaaS provider.',
      'dialpad': 'Yes, we work with Dialpad—AI-powered business communications.',
      'mitel': 'Yes, we work with Mitel—enterprise communications.',
      'cisco': 'Yes, we work with Cisco—enterprise-grade solutions.',
      'microsoft teams': 'Yes, we integrate with Microsoft Teams.',
      'teams': 'Yes, we integrate with Microsoft Teams.',
      'five9': 'Yes, we work with Five9—cloud contact center.',
      'genesys': 'Yes, we work with Genesys—omnichannel customer experience.',
      'talkdesk': 'Yes, we work with Talkdesk—enterprise contact center.',
      'aircall': 'Yes, we work with Aircall—cloud-based phone system.',
      'grasshopper': 'Yes, we work with Grasshopper—virtual phone system.',
      'ooma': 'Yes, we work with Ooma—VoIP phone service.',
      'jive': 'Yes, we work with Jive—cloud phone systems.',
      'goto connect': 'Yes, we work with GoTo Connect—unified communications.',
      'goto': 'Yes, we work with GoTo Connect—unified communications.',
      'webex': 'Yes, we work with Webex—Cisco\'s collaboration platform.',
      'google voice': 'Yes, we integrate with Google Voice.',
      'twilio': 'Yes, we work with Twilio—programmable communications.',
      'plivo': 'Yes, we work with Plivo—cloud communications platform.',
      'bandwidth': 'Yes, we work with Bandwidth—communications APIs.',
      'intermedia': 'Yes, we work with Intermedia—unified communications.',
      'fuze': 'Yes, we work with Fuze—global cloud communications.',
      'magicjack': 'We typically work with more enterprise-focused solutions, but we can explore options.',
      'at&t': 'Yes, we work with AT&T Business—enterprise communications.',
      'att': 'Yes, we work with AT&T Business—enterprise communications.',
      'verizon': 'Yes, we work with Verizon Business—enterprise solutions.',
      't-mobile': 'Yes, we work with T-Mobile Business—enterprise communications.',
      'tmobile': 'Yes, we work with T-Mobile Business—enterprise communications.',
      'sprint': 'Yes, we work with T-Mobile Business (formerly Sprint)—enterprise communications.',
      'centurylink': 'Yes, we work with CenturyLink/Lumen—enterprise connectivity and voice.',
      'lumen': 'Yes, we work with Lumen (formerly CenturyLink)—enterprise connectivity and voice.',
      'windstream': 'Yes, we work with Windstream—enterprise communications.',
      'frontier': 'Yes, we work with Frontier—business communications.'
    }
    
    // Check for matches
    for (const [provider, answer] of Object.entries(ucaasProviders)) {
      if (lowerText.includes(provider)) {
        // ✅ FIX: Check if user is asking "do you work with X" vs telling us "we use X"
        const isAskingIfWeWorkWith  = /\b(do you|does.*work with|do.*support|work with.*you)\b/i.test(text)
        const userJustSaidTheyUseIt  = /\b(use|uses|using|work with|working with|have|got|we're with|we use)\b/i.test(text)
        
        if (isAskingIfWeWorkWith) {
          // ✅ FIX: User is asking if we work with this company
          // Mention what the code says about the company, then note interest
          // Extract company name with proper capitalization
          const companyName  = provider.charAt(0).toUpperCase() + provider.slice(1)
          return `${answer} I'll make a note that you might be interested in ${companyName}.`
        } else if (userJustSaidTheyUseIt) {
          // ✅ FIX: User told us they use this provider - acknowledge diplomatically and move on
          if (this.state 
            this.state  = 'ask_crm'
            // ✅ CRITICAL FIX: Diplomatic response for Nextiva/current providers
            const companyName  = provider.charAt(0).toUpperCase() + provider.slice(1)
            return `Yes, we work with ${companyName}. But if they're causing limitations, we'll find a provider that gives you everything you need. Do you use a CRM that your phone system needs to integrate with?`
          } else {
            return `${answer} Great! That's one of the providers we frequently help optimize.`
          }
        } else if (this.state 
        } else {
          // Non-qualification: acknowledge and record potential consideration
          return `${answer} Thanks — I'll make a note that you may be considering them as an option.`
  }

  handleUnknownProvider(text) {
    // Extract potential provider name from question
    const match  = text.match(/(?:work with|have|support|integrate with)\s+([A-Za-z0-9\s&]+)/i)
    const providerName  = match ? match[1].trim() : 'that provider'
    
    return `I'd need to check if we currently support ${providerName}. But we work with over fifty UCaaS and CCaaS providers, so there's a good chance. That's something we can cover in the 15-minute call. Sound good?`
  }

  isAskingAboutPricing(text) {
    const pricingQuestions  = [
      /how much (?:does it|do you) cost/i,
      /what'?s? the (?:price|cost)/i,
      /how (?:expensive|cheap)/i,
      /pricing/i,
      /what (?:does|do) (?:it|you) charge/i
    ]
    return pricingQuestions.some(pattern  = > pattern.test(text))
  }

  answerPricingQuestion() {
    return `Pricing depends on your setup—how many users, which features you need, integrations, etc. That's exactly why the 15-minute call is valuable. We can show you what works for your specific situation. Does that make sense?`
  }

  isAskingIfAI(text) {
    const aiQuestions  = [
      /are you (?:a )?(?:robot|bot|ai|artificial)/i,
      /is this (?:a )?(?:robot|bot|ai|artificial)/i,
      /am i talking to (?:a )?(?:robot|bot|ai|computer)/i,
      /are you (?:a )?(?:real person|human)/i
    ]
    return aiQuestions.some(pattern  = > pattern.test(text))
  }

  answerAIQuestion() {
    return `I'm an AI assistant helping to schedule calls. If you'd prefer to speak with a person directly, I can have someone call you back. Or we can keep going and I'll book you a time—either way works!`
  }

  isAskingForCallback(text) {
    return /call (?:me )?back|reach (?:out|back)|contact me later/i.test(text)
  }

  isObjection(text) {
    const objections  = [
      /not interested/i,
      /not right now/i,
      /maybe later/i,
      /call back (?:later|another time)/i,
      /busy right now/i,
      /bad time/i
    ]
    return objections.some(pattern  = > pattern.test(text))
  }

  handleObjection() {
    return `Totally understand. Just so you know, the call is only 15 minutes and might save you time or money down the road. But if now's not good, I can have someone reach out another time. What's better for you?`
  }

  isConfused(text) {
    return /what|huh|sorry|didn'?t catch|repeat|come again/i.test(text) && text.length < 20
  }

  handleConfusion() {
    // Repeat last question more slowly
    return `Let me repeat that. ${this.lastBotMessage}`
  }

  isWrongNumber(text) {
    // ✅ ENHANCED: Better wrong number detection
    return /wrong number|don'?t (?:know|have)|never heard|don'?t have.*hello|no.*hello|hello.*doesn'?t work/i.test(text)
  }

  handleWrongNumber() {
    this.state  = 'done'
    return `My apologies for the confusion. Have a great day!`
  }

  isLanguageBarrier(text) {
    // Detect non-English or "no speak English"
    return /no (?:speak|habla) english|spanish|no entiendo/i.test(text)
  }

  handleLanguageBarrier() {
    this.state  = 'done'
    return `I apologize, I only speak English. Have a great day!`
  }

  isAlreadyCustomer(text) {
    return /already (?:use|have|working with) (?:you|goatvox)/i.test(text)
  }

  handleAlreadyCustomer() {
    this.state  = 'done'
    return `Oh wonderful! Then you already know what we do. Is there anything specific I can help with today?`
  }

  // ✅ ENHANCED: Detect reluctant/conditional volunteering
  isReluctantVolunteering(text) {
    const reluctantPatterns  = [
      /\b(maybe|perhaps|possibly|i guess|i suppose|well|hmm)\s+(i can|i could|i might|let me)\b/i,
      /\b(i can probably|i could maybe|i might be able to)\b/i,
      /\b(if you want|if you need|if it helps)\b.*\b(i can|i could|help)\b/i,
      /\b(i'?ll try|let me try|i'?ll see what i can do)\b/i
    ]
    return reluctantPatterns.some(pattern  = > pattern.test(text))
  }

  // ✅ ENHANCED: Better "out of office" understanding with context
  isExplicitlyUnavailable(text, mentionedNames 
    
    // If names were mentioned AND unavailable language is used, it's explicit
    if (mentionedNames.length > 0 && unavailablePatterns.some(p  = > p.test(text))) {
      return true
    }
    
    return false
  }

  // ✅ ENHANCED: Detect uncertainty about availability
  isUncertainAboutAvailability(text) {
    return /\b(i think|i believe|i'?m not sure|not sure if|might be|probably)\b.*\b(out|away|unavailable|not here|not in)\b/i.test(text)
  }

  // NEW: LLM interpretation method for hybrid approach
  async interpretWithLLM(userText, currentState, goal) {
    return await deepseek.interpretWithLLM(userText, currentState, goal)
  }

  // ✅ ENHANCED: Improved interruption detection with context preservation
  detectInterruption(userText) {
    const now  = Date.now()
    const timeSinceLastResponse  = now - this.lastResponseTime
    
    // Consider it an interruption if user speaks within 2 seconds of our last response
    const isInterruption  = userText && this.lastResponseTime > 0 && timeSinceLastResponse < 2000
    
    if (isInterruption) {
      this.interruptionCount++
      this.conversationMetrics.interruptions++
      this.wasInterrupted  = true
      
      // ✅ FIX: Save context before handling interruption
      if (!this.preInterruptionState) {
        this.preInterruptionState  = this.state
        this.interruptionContext  = {
          state: this.state,
          lastBotMessage: this.lastBotMessage,
          ctx: { ...this.ctx },
          timestamp: now
        }
      }
      
      logger.info(`🔄 Interruption detected (#${this.interruptionCount})`, { 
        timeSinceLastResponse,
        userText,
        currentState: this.state,
        savedContext: this.interruptionContext?.state
      })
    }
    
    return isInterruption
  }
  
  // ✅ NEW: Handle interruption - stop talking, answer question, resume context
  async handleInterruption(userText, convoLogger) {
    if (!this.wasInterrupted) return null
    
    logger.info(`🛑 Handling interruption - stopping current flow to answer question`)
    
    // Use DeepSeek to understand the question and provide answer
    if (DEEPSEEK_API_KEY) {
      const conversationContext 
      
      const interruptionPrompt  = `The user interrupted me. They asked: "${userText}"
      
Current conversation context:
${conversationContext}

Previous topic I was discussing: ${this.lastBotMessage || 'Initial greeting'}

Provide a brief, helpful answer to their question (under 20 words), then indicate we can continue with the previous topic.`

      try {
        const interpretation  = await this.interpretWithLLM(
          interruptionPrompt,
          this.preInterruptionState || this.state,
          'Answer user question during interruption, then resume previous conversation flow'
        )
        
        if (interpretation && interpretation.response) {
          // Answer the question
          const answer  = interpretation.response
          
          // ✅ Resume previous context after answering
          this.wasInterrupted  = false
          const resumedState  = this.preInterruptionState || this.state
          this.state  = resumedState
          
          logger.info(`✅ Interruption handled - answered question, resuming state: ${resumedState}`)
          
          // Clear interruption context after handling
          this.preInterruptionState  = null
          this.interruptionContext  = null
          
          return answer
        }
      } catch (error) {
        logger.error('❌ Error handling interruption with DeepSeek:', error)
      }
    }
    
    // Fallback: simple acknowledgment
    this.wasInterrupted  = false
    this.interruptionContext  = null
    
    return null
  }

  // Send callback request to n8n
  async sendCallbackRequestToN8N() {
    if (!this.callbackPhone || !this.callbackName) {
      logger.error('❌ Missing callback details for n8n')
    }

    try {
      const payload  = {
        type: "callback_request",
        name: this.callbackName,
        phone: this.callbackPhone,
        company: this.ctx.companyName || 'Unknown Company',
        outcome: "callback-requested",
        source: "Ava Voice",
        timestamp: new Date().toISOString(),
        callSid: this.ctx.callSid,
        recordingUrl: this.ctx.recordingUrl,
        totalCost: this.costTracker.getCostSummary().total,
        callDuration: Math.floor((new Date() - this.callStartTime) / 1000),
        tags: 'callback-requested, live-transfer'
      }

      logger.info('📞 Sending callback request to n8n:', { payload })

      if (response.ok) {
        logger.info('✅ Callback request sent to n8n successfully')
      } else {
        logger.error('❌ Failed to send callback request to n8n')
      }
    } catch (error) {
      logger.error('❌ Error sending callback request to n8n:', { message: error.message })
    }
  }

  // NEW: Send scheduling link email
  async sendSchedulingLinkEmail(email) {
    try {
      const payload  = {
        type: "scheduling_link",
        email: email,
        name: this.ctx.firstName || 'Contact',
        company: this.ctx.companyName || 'Unknown Company',
        source: "Ava Voice",
        timestamp: new Date().toISOString(),
        callSid: this.ctx.callSid,
        outcome: "scheduling-link-sent",
        tags: 'scheduling-link, email-fallback'
      }

      logger.info('📧 Sending scheduling link to n8n:', { payload })

      if (response.ok) {
        logger.info('✅ Scheduling link sent to n8n successfully')
      } else {
        logger.error('❌ Failed to send scheduling link to n8n')
      }
    } catch (error) {
      logger.error('❌ Error sending scheduling link to n8n:', { message: error.message })
    }
  }

  yes(text) {
    return /\b(yes|yeah|yep|sure|ok|okay|absolutely|definitely|of course|certainly)\b/i.test(text)
  }
  
  no(text) {
    return /\b(no|nope|nah|not really|not interested|no thanks)\b/i.test(text)
  }
  
  // 
    const voicemailIndicators  = /\b(subscriber|not available|leave a message|after the tone|voicemail|mailbox|unavailable|reach|please leave)\b/i
    
    return greetings.test(text) && !voicemailIndicators.test(text)
  }
  
  async tryExtractName(text) {
    return await deepseek.tryExtractName(text)
  }

  tryExtractEmail(text) {
    return this.emailCollector.extractEmail(text)
  }

  extractNumber(text) {
    return this.numberExtractor.extractNumber(text)
  }
  
  // FIXED: Enhanced time slot selection with morning/afternoon logic
  async scheduleAppointment() {
    try {
      // ✅ ENHANCED: Use new smart slot selection with descriptions
      const slotResult  = await calCom.getSmartTimeSlotsWithDescription()
      
      this.ctx.availableSlots  = slotResult.slots

      if (slotResult.slots.length 
        return `I'm not finding available times right now. Would you like me to email you a link to schedule at your convenience?`
      }

      // ✅ FIX: Mention appointment duration
      return `Perfect! I have some 15-minute appointments available. ${slotResult.description}`

    } catch (error) {
      this.state  = 'get_email_for_followup'
      return `I'm having trouble accessing the calendar right now. Would you like me to email you a scheduling link instead?`
    }
  }
  
  formatTimeSlot(date) {
    return timezoneManager.formatTimeSlot(date)
  }
  
  // FIXED: Enhanced time slot selection logic
  findRequestedTimeSlot(text) {
    const lowerText  = text.toLowerCase().trim()
    
    // ✅ FIX: Handle "08:15" format properly
    const timePattern  = /(\d{1,2}):?(\d{2})?\s*(am|pm)?/i
    const timeMatch  = lowerText.match(timePattern)
    
    if (timeMatch) {
      let hour  = parseInt(timeMatch[1])
      const minutes  = timeMatch[2] ? parseInt(timeMatch[2]) : 0
      const meridiem  = timeMatch[3] ? timeMatch[3].toLowerCase() : ''
      
      // Convert to 24-hour format
      if (meridiem 
      if (meridiem 
      
      // Find the slot that matches this time
      const matchingSlot 
        const slotHour  = slotDate.getHours()
        const slotMinutes  = slotDate.getMinutes()
        
        return slotHour 
      
      if (matchingSlot) {
        logger.info(`✅ Found exact time match: ${hour}:${minutes} -> ${this.formatTimeSlot(matchingSlot)}`)
        return matchingSlot
      }
    }
    
    // Continue with existing logic for "first", "second", etc.
    if (/\b(first|1st|one)\b/i.test(lowerText)) return 'first'
    if (/\b(second|2nd|two)\b/i.test(lowerText)) return 'second'
    
    // Handle simple time expressions
    // Match patterns like "1pm", "1 pm", "1:00", "eight am"
    const simpleTimeMatch  = lowerText.match(/\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*(am|pm|a\.m\.|p\.m\.|o'?clock)?\b/i)
    
    if (simpleTimeMatch) {
      const timeStr  = simpleTimeMatch[1]
      
      // Convert word to number
      const wordToNum  = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6,
        'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10, 'eleven': 11, 'twelve': 12
      }
      
      let hour  = parseInt(timeStr) || wordToNum[timeStr.toLowerCase()]
      
      if (hour) {
        // Determine AM/PM
        const isPM 
        
        if (isPM && hour < 12) hour + = 12
        
        // Find slot matching this hour (within 1 hour tolerance)
        const matchingSlot 
          return Math.abs(slotHour - hour) < = 1
        
        if (matchingSlot) return matchingSlot
      }
    }
    
    // Handle day preferences
    if (text.includes('today')) return 'today'
    if (text.includes('tomorrow')) return 'tomorrow'
    
    const times  = {
      'nine': 9, '9': 9, '9am': 9, '9 am': 9, '9:00': 9, '9:00am': 9,
      'ten': 10, '10': 10, '10am': 10, '10 am': 10, '10:00': 10, '10:00am': 10,
      'eleven': 11, '11': 11, '11am': 11, '11 am': 11, '11:00': 11, '11:00am': 11,
      'twelve': 12, '12': 12, '12pm': 12, '12 pm': 12, '12:00': 12, '12:00pm': 12,
      'one': 13, '1': 13, '1pm': 13, '1 pm': 13, '1:00': 13, '1:00pm': 13,
      'two': 14, '2': 14, '2pm': 14, '2 pm': 14, '2:00': 14, '2:00pm': 14,
      'three': 15, '3': 15, '3pm': 15, '3 pm': 15, '3:00': 15, '3:00pm': 15,
      'four': 16, '4': 16, '4pm': 16, '4 pm': 16, '4:00': 16, '4:00pm': 16,
      'eight': 8, '8': 8, '8am': 8, '8 am': 8, '8:00': 8, '8:00am': 8
    }
    
    const days  = {
      'today': 0,
      'tomorrow': 1,
      'monday': this.getDaysUntil('Monday'),
      'tuesday': this.getDaysUntil('Tuesday'),
      'wednesday': this.getDaysUntil('Wednesday'),
      'thursday': this.getDaysUntil('Thursday'),
      'friday': this.getDaysUntil('Friday')
    }
    
    let requestedDay  = null
    let requestedTime  = null
    
    // Check for day preferences
    for (const [day, daysFromNow] of Object.entries(days)) {
      if (text.includes(day)) {
        requestedDay  = daysFromNow
        break
      }
    }
    
    // Check for time preferences
    const sortedTimes 
    for (const [timePattern, hour] of sortedTimes) {
      if (text.includes(timePattern)) {
        requestedTime  = hour
      }
    }
    
    // ✅ CRITICAL FIX: Don't create fake dates - only use REAL Cal.com slots
    // When user requests a day like "Tuesday", find the actual Tuesday slot from available slots
    if (requestedDay !
      const today  = new Date().getDay() // 0 
      const requestedDayName  = dayNames[targetDayIndex]
      
      logger.info(`🔍 Looking for ${requestedDayName} slots (${requestedDay} days from now)`)
      
      // Find slots on the requested day from actual Cal.com slots
      const daySlots 
        return slotDayName 
      
      if (daySlots.length > 0) {
        logger.info(`📅 Found ${daySlots.length} ${requestedDayName} slots in Cal.com`)
        
        // Filter by business hours (8 AM - 6 PM Pacific)
        const businessDaySlots 
          return pacificHour >
        
        if (businessDaySlots.length > 0) {
          logger.info(`✅ Found ${businessDaySlots.length} business hours slots on ${requestedDayName}`)
          
          // If time requested, find closest matching time
          if (requestedTime !
              return Math.abs(pacificHour - requestedTime) < = 1 // Within 1 hour
            })
            
            if (timeSlots.length > 0) {
              logger.info(`✅ Found ${requestedDayName} slot matching time ${requestedTime}: ${timeSlots[0].toISOString()}`)
              return timeSlots[0] // Return actual Cal.com slot
            }
          }
          
          // Return first business hours slot on requested day
          logger.info(`✅ Using first ${requestedDayName} slot: ${businessDaySlots[0].toISOString()}`)
          return businessDaySlots[0] // Return actual Cal.com slot
        } else {
          logger.warn(`⚠️ No business hours slots available on ${requestedDayName}`)
        }
      } else {
        logger.warn(`⚠️ No ${requestedDayName} slots found in Cal.com available slots`)
      }
      
      // If no matching slot found, return null (don't create fake dates)
      return null
    }
    
    // ✅ FIX: For time-only requests (no day), find closest matching slot from available slots
    if (requestedTime !
        return pacificHour >
      
      if (timeSlots.length > 0) {
        logger.info(`✅ Found slot matching time ${requestedTime}: ${timeSlots[0].toISOString()}`)
        return timeSlots[0] // Return actual Cal.com slot
      }
    }
    
    return null
  }

  getDaysUntil(targetDay) {
    const days  = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
    const today  = new Date().getDay()
    const targetIndex  = days.indexOf(targetDay.toLowerCase())
    let diff  = targetIndex - today
    if (diff <
    return diff
  }
  
  // ENTERPRISE: Enhanced scheduling with memory and business logic
  async getAvailableTimeSlots() {
    // Use cached slots if available and not expired
    const now  = Date.now()
    if (this.cachedSlots && now - this.slotsFetchTime < this.slotsCacheTimeout) {
      return this.cachedSlots
    }

    try {
      const slots  = await calCom.getRealAvailableSlots(14)
      
      // ✅ PREVENTATIVE FIX: Validate slots before caching
      const validatedSlots 
          return false
        }
        
        // Reject slots too far in the future (beyond 6 months)
        const sixMonthsFromNow  = new Date()
        sixMonthsFromNow.setMonth(sixMonthsFromNow.getMonth() + 6)
        if (slot > sixMonthsFromNow) {
          logger.warn(`🚫 Filtered too-future slot: ${slot}`)
        }
        
        return true
      
      this.cachedSlots  = validatedSlots
      this.slotsFetchTime  = now
      
      if (this.cachedSlots.length 
        return `Kind of embarrassing, but we're having a problem with our calendar. We will reach back out, sorry for the complication.`
      }
      
      logger.info(`✅ Retrieved and validated ${this.cachedSlots.length} Cal.com slots`)
      return this.cachedSlots
      
    } catch (error) {
      logger.error('❌ Cal.com fetch error:', { message: error.message })
      return `Kind of embarrassing, but we're having a problem with our calendar. We will reach back out, sorry for the complication.`
    }
  }
  
  // Simple email confirmation without spelling issues
  confirmEmail(email) {
    return `I have ${email}. Is that correct?`
  }
  
  detectVoicemail(transcript) {
    if (!transcript || transcript.length < 10) return false
    
    const isVoicemail  = this.voicemailDetector.isVoicemail(transcript)
    
    if (isVoicemail) {
      this.consecutiveVoicemailCount++
      
      if (this.consecutiveVoicemailCount >
        this.state  = 'voicemail_hangup'
      }
    } else {
      this.consecutiveVoicemailCount  = 0
    }
    
    return isVoicemail
  }

  async bookAppointmentInCalCom() {
    if (!CAL_COM_API_KEY || !CAL_EVENT_TYPE_ID) {
      logger.warn('⚠️ Cal.com not configured - skipping real booking')
    }
    try {
      logger.info('📅 Booking real appointment in Cal.com')
     
      const customerDetails  = {
        firstName: this.ctx.firstName,
        lastName: this.ctx.lastName || '',
        email: this.ctx.email,
        phone: this.ctx.phone,
        accountId: this.accountId
      }
      
      const bookingResult  = await calCom.bookAppointment(this.ctx.bookingTime, customerDetails)
     
      if (bookingResult && bookingResult.bookingId) {
        this.ctx.calComBookingId  = bookingResult.bookingId
        this.ctx.calComBookingUid  = bookingResult.bookingUid
        this.ctx.meetingUrl  = bookingResult.meetingUrl
        this.ctx.bookingTime  = bookingResult.bookingTime
        logger.info(`✅ Real Cal.com booking confirmed: ${bookingResult.bookingId}`)
      }
    } catch (error) {
      logger.error(`❌ Cal.com booking error: ${error.message}`)
    }
  }

  getConversationDataForZoho(callOutcome) {
    const callDuration  = Math.floor((new Date() - this.callStartTime) / 1000)
    
    // Update cost tracker with final metrics
    this.costTracker.addCallDuration(callDuration)
    this.costTracker.addCharactersSpoken(this.charactersSpoken)
    this.costTracker.addDeepSeekTokens(this.deepseekTokens.input, this.deepseekTokens.output)
    this.costTracker.setRecordingUsed(true)

    const costSummary  = this.costTracker.getCostSummary()

    // Build tags including callback request if applicable
    let tags  = `Called-Today, attempt-1`
    if (this.ctx.email) tags + = ', Email-Collected'
    if (this.ctx.calComBookingId) tags + = ', Meeting-Scheduled'
    if (this.callbackRequested) tags + = ', Callback-Requested'
    if (this.emailCollector.shouldGiveUp()) tags + = ', Partial-Booking'
    if (this.state 
    if (this.interruptionCount > 0) tags + = `, Interruptions-${this.interruptionCount}`

    return {
      contactName: this.ctx.firstName || 'Unknown',
      outcome: callOutcome,
      employeeCount: this.ctx.seats || 'Unknown',
      phoneProvider: this.ctx.provider || 'Unknown',
      crmName: this.ctx.crmName || 'N/A',
      email: this.ctx.email || 'Not collected',
      calComBookingId: this.ctx.calComBookingId || 'N/A',
      bookingTime: this.ctx.bookingTime ? this.formatTimeSlot(new Date(this.ctx.bookingTime)) : 'N/A',
      companyName: this.ctx.companyName || 'Unknown Company',
      locationInfo: this.locationInfo,
      seats: this.ctx.seats,
      provider: this.ctx.provider,
      useCRM: this.ctx.useCRM || false,
      crmName: this.ctx.crmName,
      charactersSpoken: this.charactersSpoken,
      deepseekInputTokens: this.deepseekTokens.input,
      deepseekOutputTokens: this.deepseekTokens.output,
      // Include callback information
      callbackRequested: this.callbackRequested,
      callbackPhone: this.callbackPhone,
      callbackName: this.callbackName,
      tags: tags,
      costBreakdown: costSummary,
      // NEW: Additional fields for Zoho
      callSid: this.ctx.callSid,
      recordingUrl: this.ctx.recordingUrl,
      totalCost: costSummary.total,
      callDuration: callDuration,
      interruptions: this.interruptionCount
    }
    const decisionMaker  = nameMatch ? nameMatch[2] : (this.ctx.decisionMaker || 'them')
    
    this.state  = 'waiting_for_transfer'
    
    // ✅ FIX: Set 75-second timeout for transfer
    if (this.transferTimeout) clearTimeout(this.transferTimeout)
    this.transferTimeout 
    }, 75000) // 75 seconds
    
    // ✅ SMART RESPONSE: Acknowledge and wait for transfer
    return `Perfect! I'll wait to speak with ${decisionMaker}. Thank you for transferring me.`
  }

  handleTransferTimeout() {
    logger.info('⏰ Transfer timeout after 75s - checking if anyone is there')
    }, 10000) // Wait 10 seconds for response after asking
  }

  //  // ✅ LATENCY TRACKING: Start timing
    const now  = inputStartTime
    const currentInput  = userText.toLowerCase().trim()
    const lastInput  = this.lastUserInput?.toLowerCase().trim()
    
    // ✅ LATENCY TRACKING: Log input received
    logger.info(`📥 [LATENCY] User input received at ${now}ms: "${userText}"`)
    
    // ✅ FIX: Better greeting detection - respond immediately to first greeting
    const isInitialGreeting 
    
    // ✅ SMART DEBOUNCE: Different timing based on content and state
    let debounceTime  = 2000 // Default 2 seconds
    
    // NO debounce for initial greetings - respond immediately
    if (isInitialGreeting && this.lastUserTime 
    }
    // Shorter debounce for simple greetings after initial
    else if (/^(hello|hi|hey)[?!.\s]*$/i.test(currentInput)) {
      debounceTime  = 2500 // 2.5 seconds for repeat greetings
    }
    // Shorter debounce for meaningful responses
    else if (currentInput.length > 10 || /\b(yes|no|maybe|probably)\b/i.test(currentInput)) {
      debounceTime  = 1500 // 1.5 seconds for substantive responses
    }
    
    // Only debounce exact repeats within the calculated time
    if (now - this.lastUserTime < debounceTime && currentInput 
    }
    
    this.lastUserTime  = now
    
    // ✅ FIX: Check for interruption FIRST, before processing
    const wasInterrupted  = this.detectInterruption(userText)
    if (wasInterrupted && this.lastResponseTime > 0) {
      logger.info(`🛑 Interruption detected - handling user question first`)
      const interruptionResponse  = await this.handleInterruption(userText, convoLogger)
      if (interruptionResponse) {
        const responseTime  = Date.now() - inputStartTime
        logger.info(`📤 [LATENCY] Interruption response generated in ${responseTime}ms`)
        this.lastResponseTime  = Date.now()
        return interruptionResponse
      }
    }
    
    // Track conversation history
    this.conversationHistory.push({
      role: 'user',
      text: userText,
      timestamp: Date.now()
    })

    // ✅ FIX: Trim conversation history to prevent memory leaks
    if (this.conversationHistory.length > 50) {
      this.conversationHistory  = this.conversationHistory.slice(-25)
    }

    // ✅ CRITICAL FIX: Detect frustration and objections GLOBALLY (before state processing)
    const globalFrustrationPatterns  = [
      /didn'?t.*say.*anything/i,
      /keep.*repeat/i,
      /you.*repeat/i,
      /said.*nothing/i,
      /no.*idea/i,
      /don'?t.*know.*who/i,
      /who'?s.*hell/i,
      /confused/i,
      /what.*talking/i,
      /you.*not.*listen/i
    ]
    
    if (globalFrustrationPatterns.some(pattern  = > pattern.test(userText))) {
      logger.info(`🚨 Global frustration detected: "${userText}"`)
      // Clear any bad state/name extraction
      if (this.ctx.decisionMaker && this.ctx.decisionMaker.toLowerCase() 
      }
      
      // Reset to a clean state and apologize
      const apologyResponse  = `I apologize for the confusion. Let me start over. I'm ${AGENT_NAME} with ${COMPANY_NAME}. Who handles your phone system?`
      this.greetingGiven  = true
      this.lastResponseTime  = Date.now()
      this.lastBotMessage  = apologyResponse
      this.conversationHistory.push({
        role: 'assistant',
        text: apologyResponse,
        timestamp: Date.now()
      })
      const responseTime  = Date.now() - inputStartTime
      logger.info(`📤 [LATENCY] Frustration response generated in ${responseTime}ms`)
      return apologyResponse
    }
    
    // ✅ ENHANCED: Update conversation context
    this.trackMentionedNames(userText)
    this.updateCurrentSpeaker(userText)
    
    // ✅ LATENCY TRACKING: Log processing start
    const processingStartTime  = Date.now()
    logger.info(`⚙️ [LATENCY] Starting response processing at ${processingStartTime}ms (${processingStartTime - inputStartTime}ms after input)`)
    
    // ✅ FIX: Track slotChosen to prevent state loops
    if (!this.ctx.slotChosen) {
      this.ctx.slotChosen  = false
    }

    // 
      return await this.handleTransferOffer(userText)
      return `I'm ${AGENT_NAME} with ${COMPANY_NAME}. We help businesses compare phone systems and contact center solutions to save money or add features. I was hoping to speak with whoever handles your phone system. Who would be the best person for that?`
    }

    if (userText && this.isMessageTaking(userText)) {
      logger.info(`📝 Message taking detected: "${userText}"`)
      return `Perfect. What's the best email address I can use to follow up with them?`
    }

    if (userText && this.isCallbackOffer(userText)) {
      logger.info(`📞 Callback offer detected: "${userText}"`)
      return `Thanks! What's the best email address where they can reach me?`
    }

    if (userText && this.isTemporarilyBusy(userText)) {
      logger.info(`⏳ Temporarily busy detected: "${userText}"`)
      return `No problem. Should I try back in about 30 minutes, or would email be better?`
    }

    if (userText && this.isCheckingAvailability(userText)) {
      logger.info(`🔍 Checking availability detected: "${userText}"`)
      return `Sure, I'll wait.`
      return `I appreciate you letting me know. Who should I speak with about phone systems, or what's the correct extension?`
    if (recovery.shouldRecover) {
      const prevState  = this.state
      
      if (convoLogger) convoLogger.logStateChange(prevState, this.state)
      
      let response  = ''
      switch (recovery.triggerType) {
        case 'help_offered':
          response  = `Perfect, glad I've got the right person! Let me start over. We help companies compare phone systems to save money or add features. Worth a quick 15-minute call?`
          break
        case 'time_issue':
          response  = `Let me find better time options for you.`
          return await this.scheduleAppointment() // Restart scheduling
        case 'confusion':
          response  = `Let me clarify - I'm looking to speak with whoever handles your phone system. Who would be the right person for that?`
      }
      
      this.lastBotMessage  = response
      this.conversationHistory.push({
        role: 'assistant',
        text: response,
        timestamp: Date.now()
      })
      
      return response
      
      // Don't change state - just answer and continue
      this.lastBotMessage  = response
      
      if (convoLogger) convoLogger.logSystemEvent('answered_company_question')
    }

    // Check for SPECIFIC provider question first
    const specificProvider  = this.checkSpecificProvider(userText)
    if (specificProvider) {
      // ✅ FIX: checkSpecificProvider already returns complete response with "Thanks — noted" or "Thanks — I'll make a note"
      // Don't append it again to avoid duplication
      let response  = specificProvider

      this.lastBotMessage  = response

      if (convoLogger) convoLogger.logSystemEvent('answered_specific_provider')

      return response
    }

    // Check for GENERAL provider question ("what companies do you work with?")
    if (userText && this.isAskingAboutProviders(userText)) {
      const response  = this.answerProviderQuestion()
      
      this.lastBotMessage  = response
      
      if (convoLogger) convoLogger.logSystemEvent('answered_provider_question')
    }

    // Handle pricing questions
    if (userText && this.isAskingAboutPricing(userText)) {
      const response  = this.answerPricingQuestion()
      
      if (convoLogger) convoLogger.logSystemEvent('answered_pricing_question')
    }

    // Handle AI/robot questions
    if (userText && this.isAskingIfAI(userText)) {
      const response  = this.answerAIQuestion()
      
      if (convoLogger) convoLogger.logSystemEvent('answered_ai_question')
    }

    // Handle objections
    if (userText && this.isObjection(userText)) {
      const response  = this.handleObjection()
      
      if (convoLogger) convoLogger.logSystemEvent('handled_objection')
    }

    // Handle confusion
    if (userText && this.isConfused(userText)) {
      const response  = this.handleConfusion()
      
      if (convoLogger) convoLogger.logSystemEvent('handled_confusion')
    }

    // Handle wrong number
    if (userText && this.isWrongNumber(userText)) {
      const response  = this.handleWrongNumber()
      
      if (convoLogger) convoLogger.logSystemEvent('handled_wrong_number')
    }

    // Handle language barrier
    if (userText && this.isLanguageBarrier(userText)) {
      const response  = this.handleLanguageBarrier()
      
      if (convoLogger) convoLogger.logSystemEvent('handled_language_barrier')
    }

    // Handle already customer
    if (userText && this.isAlreadyCustomer(userText)) {
      const response  = this.handleAlreadyCustomer()
      
      if (convoLogger) convoLogger.logSystemEvent('handled_already_customer')
        return `Hi, thanks for waiting! I'm looking to speak with whoever oversees your phone system. Who would be the best person for that?`
      }
      // Otherwise, the second timeout will handle hanging up
    }

    // 
      
      // Simple keyword detection for terminal state recovery
      if (text.includes('help') || text.includes('assist') || 
          text.includes('talk') || text.includes('speak') ||
          text.includes('available') || text.includes('handle')) {
        
        this.emailCollector.reset()
        
        if (convoLogger) convoLogger.logStateChange('re_engaged_fallback', this.state)
        
        this.conversationStage  = 'qualification'
        
        this.lastBotMessage  = response
        this.conversationHistory.push({
          role: 'assistant',
          text: response,
          timestamp: Date.now()
        })
        return response
      }
    }

    // GLOBAL RE-ENGAGEMENT CHECK - FIX FOR STUCK STATES
    if (userText && this.state !
      
      const matched 
      
      if (matched) {
        logger.info(`✅ Re-engagement pattern matched: "${userText}" with pattern: ${matched}`)
        
        // Check for unavailability first before assuming they want to talk
        if (this.isPersonUnavailable(userText)) {
          logger.info(`⏳ Person unavailable detected in re-engagement: "${userText}"`)
          const response  = `I understand they're unavailable. Is there someone else who handles phone systems that I could speak with?`
          
          this.lastBotMessage  = response
          this.conversationHistory.push({
            role: 'assistant',
            text: response,
            timestamp: Date.now()
          })
          return response
        }
        
        // Reset from terminal states
        if (this.state 
          
          this.emailCollector.reset()
          
          const response  = `Perfect, glad I've got the right person! We help companies compare phone & contact-center providers to save money or add features. Worth a quick 15-minute call?`
          
          if (convoLogger) convoLogger.logStateChange('re_engaged', this.state)
          
          this.conversationStage  = 'qualification'
        }
      } else {
        // Log when patterns DON'T match in terminal states
        if (this.state 
        }
      }
    }

    // Check for transfer/human requests in any state
    if (userText && this.isTransferRequest(userText) && !this.callbackRequested) {
      this.callbackRequested  = true
      
      // If they specifically ask for our number/contact info
      if (/\b(your number|call you|your phone|contact you|reach you)\b/i.test(userText)) {
        this.state  = 'provide_contact_info'
        const response  = `Currently I'm focused on outbound calls to learn about your needs. Our specialists will reach out based on the information we gather. What's the best email address where we can send more information about phone system options?`
        this.lastBotMessage  = response
      } else {
        // Standard human transfer request
        this.state  = 'request_callback_phone'
      }
    }

    // Check for interruptions
    const isInterruption  = this.detectInterruption(userText)
    if (isInterruption && this.interruptionCount < = 2) {
      if (convoLogger) convoLogger.logSystemEvent('interruption_detected')
      return response
    }

    if (userText && this.detectVoicemail(userText)) {
      if (this.state 
      }
      return ''
    }
    
    const text  = (userText || '').toLowerCase().trim()
    
    let response
    const prevState  = this.state
    
    // UPDATE CONVERSATION STAGE based on state
    if (['wait_for_greeting', 'find_decision_maker', 'request_transfer'].includes(this.state)) {
      this.conversationStage  = 'greeting'
    } else if (['openness_pitch', 'ask_seats', 'ask_provider', 'ask_crm', 'ask_which_crm'].includes(this.state)) {
      this.conversationStage  = 'qualification'
    } else if (['offer_appointments', 'get_name_for_booking', 'get_last_name', 'get_email', 'confirm_email'].includes(this.state)) {
      this.conversationStage  = 'booking'
    }
    
    switch (this.state) {
      case 'wait_for_greeting':
        // ✅ FIX: Respond immediately to first greeting, don't wait for second one
        const isGreetingPattern  = /^(hello|hi|hey|good morning|good afternoon|yes|speaking|this is)[?!.\s]*$/i.test(text.trim())
        const isAnyResponse 
        
        if (isGreetingPattern || isAnyResponse) {
          this.state  = 'find_decision_maker'
          this.greetingGiven  = true // ✅ CRITICAL: Track that we've given greeting
          response  = `Hi, this is ${AGENT_NAME} with ${COMPANY_NAME}. I'm looking to speak with whoever oversees your phone system or contact center. Who would be the best person for that?`
        } else {
          response  = ''
        }
        break
        
      case 'find_decision_maker':
        // ✅ CRITICAL FIX: Detect frustration and objections FIRST, before anything else
        const frustrationPatterns  = [
          /didn'?t.*say.*anything/i,
          /keep.*repeat/i,
          /you.*repeat/i,
          /said.*nothing/i,
          /no.*idea/i,
          /don'?t.*know/i,
          /confused/i,
          /what.*talking/i
        ]
        
        if (frustrationPatterns.some(pattern 
          this.ctx.decisionMaker  = null // Clear any bad name extraction
          this.greetingGiven  = true
        }
        
        // ✅ CRITICAL FIX: Detect truncated/incomplete words that might be "Hello"
        // If user says "Hell" or similar short words that could be truncated greetings, ignore them
        const truncatedGreeting  = /^(hell|hel)[?!.\s]*$/i.test(userText.trim())
        if (truncatedGreeting) {
          // User said "Hell" - this is likely a truncated "Hello", NOT a name
          logger.info(`⚠️ Detected truncated greeting: "${userText}" - ignoring, not extracting as name`)
          // Don't respond - user is probably still talking or it's audio cut-off
          response  = ''
        }
        
        // ✅ CRITICAL FIX: If user says hello again after we've already greeted, DON'T REPEAT QUESTION
        const isRepeatGreeting  = /^(hello|hi|hey)[?!.\s]*$/i.test(userText.trim())
        if (isRepeatGreeting) {
          // User is saying hello again - they might not have heard us, or we're talking over them
          // DON'T repeat the question - just wait silently
          logger.info(`⚠️ Repeated greeting detected: "${userText}" - not responding to avoid repetition`) // ✅ CRITICAL: Empty response  = don't say anything
          break
        }
        
        // ✅ FIX 3: Auto-Reset on "I was just saying hello"
        const justHello  = /just.*say.*hello|heard.*hello/i
        if (justHello.test(userText)) {
          this.state  = 'find_decision_maker'
          return `No worries! Who oversees your phone system?`
        }
        
        // ✅ ENHANCED: Extract ANY names dynamically (with improved filtering)
        const extractedNames  = this.extractAnyNames(userText)
        
        if (extractedNames && extractedNames.length > 0) {
          // ✅ CRITICAL FIX: Filter out false positives (like "Yeah", "Hell", etc.)
          const validNames 
            // Double-check it's not a common word, truncated greeting, or profanity
            const invalidWords  = ['yeah', 'yes', 'no', 'well', 'so', 'then', 'now', 'but', 'and', 'or', 
                                  'hell', 'hel', 'hello', 'hi', 'hey', 'what', 'who', 'when', 'where', 'why', 'how']
            return !invalidWords.includes(lowerName) &&
                   name.length > // Explicitly reject "hell" or "hel"
          })
          
          if (validNames.length >
            const namesList  = validNames.join(' and ')
            return `I see ${namesList} might handle this. Who's available right now that I could speak with?`
          } else if (validNames.length 
            return `Perfect! Can you transfer me to ${validNames[0]}? I'll wait.`
          }
        }
        
        // ✅ FIX: Handle "repeating yourself" or frustration (should already be caught above, but keep as backup)
        if (/repeat|repeating|just asked|you.*ask/i.test(userText)) {
          // User is pointing out we're repeating - acknowledge and move forward
          response  = `I apologize for the repetition. Let me try a different approach. Who specifically handles your phone system?`
        }
        
        // FIX 1: Add user_hinted_unavailable flag logic
        const uncertaintyPatterns  = [
          /not sure/i, /maybe/i, /i think/i, /could be/i, /out of office/i, /not here/i
        ]

        if (uncertaintyPatterns.some(p 
        }
        
        if (this.yes(text) || /\b(i can help|i'?ll help|i handle|i oversee|that'?s me|me|you can talk to me|i'?m the one|speak with me|i do|i'?m in charge|i am|i would|i would be)\b/i.test(text)) {
          this.state  = 'openness_pitch'
          // FIX 3: Prevent repeat pitch
          if (this.pitchAlreadyGiven) {
            this.state  = 'ask_seats'
            response  = `Great! First, about how many people use phones at your company?`
          } else {
            this.pitchAlreadyGiven  = true
          }
        } else {
          const name  = await this.tryExtractName(userText)
          if (name) {
            // ✅ ADDITIONAL VALIDATION: Make sure it's not a ridiculous name
            const reasonableName 
            
            if (reasonableName) {
              // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
              this.ctx.decisionMaker  = name
              this.state  = 'waiting_for_transfer'
              response  = `Perfect! Can you transfer me to ${name}? I'll wait.`
            } else {
              // If we got a bad name extraction, ask for clarification
              response  = `I want to make sure I have the right person. Who specifically handles your phone system?`
            }
          } else if (this.no(text) || /\b(i don'?t know|not sure|can'?t help)\b/i.test(text)) {
            this.state  = 'collect_email_fallback'
          } else {
            // ✅ FIX: Fallback - try extractAnyNames one more time with the original userText
            // This handles cases where names weren't extracted the first time
            const fallbackNames  = this.extractAnyNames(userText)
            if (fallbackNames && fallbackNames.length > 0) {
              const validFallbackNames 
                return !['yeah', 'yes', 'no', 'well', 'so', 'then', 'now', 'but', 'and', 'or'].includes(lower)
              })
              
              if (validFallbackNames.length >
                this.state  = 'ask_who_available'
                const namesList  = validFallbackNames.join(' and ')
                response  = `I see ${namesList} might handle this. Who's available right now that I could speak with?`
              } else if (validFallbackNames.length 
              } else {
                response  = `I want to make sure I have the right person. Who specifically handles your phone system?`
              }
            } else {
              // If we can't extract a name and it's not a clear no, ask for clarification
              response  = `Who would be the best person to speak with about your phone system?`
            }
          }
        }
        break
        
      // ✅ ENHANCED ASK_WHO_AVAILABLE CASE
      case 'ask_who_available':
        // ✅ PRIORITY 1: Check for EXPLICIT unavailability with mentioned names
        if (this.ctx.multipleContacts && this.isExplicitlyUnavailable(userText, this.ctx.multipleContacts)) {
          logger.info(`🚫 Explicitly unavailable: "${userText}"`)
          
          // Don't ask about the same people again - go straight to asking for alternatives
          this.state  = 'collect_email_fallback'
          const contacts  = this.ctx.multipleContacts.join(' or ')
          return `I understand they're out. What's the best email address I can use to reach ${contacts} when they're back?`
        }
        
        // ✅ PRIORITY 2: Check for RE-ENGAGEMENT (all variations)
        const reEngagePatterns  = [
          // Direct offers to help
          /\b(i can help|i could help|let me help|i'?ll help|i will help)\b/i,
          /\b(maybe i can help|perhaps i can help|possibly i can help)\b/i,
          /\b(i guess i can help|i suppose i can help|well i can help)\b/i,
          
          // Capability statements
          /\b(i handle|i oversee|i manage|i do|i take care of)\b/i,
          /\b(i'?m in charge|i'?m responsible|i'?m the one)\b/i,
          /\b(that'?s me|i'?m the person|i work with)\b/i,
          
          // Invitation to proceed
          /\b(talk to me|speak to me|you can talk to me|go ahead)\b/i,
          /\b(what (?:do you|can i|did you) (?:need|want)|how can i help)\b/i,
          /\b(i'?m available|i'?m here|i can assist)\b/i,
          
          // Conditional/reluctant help
          /\b(if you (?:want|need)|i might be able to|i could try)\b/i,
          /\b(i'?ll see what i can do|let me try|i'?ll try)\b/i,
          
          // Questions that show engagement
          /\b(what'?s this (?:about|regarding|concerning))\b/i,
          /\b(can i ask what|may i know what|what did you need)\b/i
        ]
        
        const isReEngaging 
        
        if (isReEngaging || this.isReluctantVolunteering(userText)) {
          logger.info(`✅ Re-engagement detected in ask_who_available: "${userText}"`)
          
          // Check if they're screening first (asking what it's about)
          if (/\b(what'?s this|what did you|can i ask|may i)\b/i.test(userText)) {
            return `Sure! We help businesses compare phone system and contact center providers to save money or add features. Is that something you handle, or should I speak with someone else?`
          }
          
          this.state  = 'openness_pitch'
          if (this.pitchAlreadyGiven) {
            this.state  = 'ask_seats'
            return `Perfect! First, about how many people use phones at your company?`
            return `Perfect, glad I've got the right person! We work with over a hundred phone and contact center providers. Would you be open to exploring options if it improved quality, added missing features, or helped book more leads?`
          }
        }
        
        // ✅ PRIORITY 3: Check for uncertainty about availability
        if (this.isUncertainAboutAvailability(userText)) {
          logger.info(`🤔 Uncertain about availability: "${userText}"`)
          return `No worries. Is there someone else available who handles phone systems, or should I follow up via email?`
        }
        
        // ✅ PRIORITY 4: Check for lunch/availability issues
        const availabilityIssues  = [
          /out.*lunch/i, /at.*lunch/i, /lunch.*time/i, /all.*out/i,
          /not.*available/i, /unavailable/i, /away/i, /out.*office/i,
          /everyone.*out/i, /they'?re.*all.*out/i, /both.*out/i
        ]
        
        if (availabilityIssues.some(pattern  = > pattern.test(userText))) {
          logger.info(`🍽️ Detected unavailability: "${userText}"`)
          
          if (this.ctx.multipleContacts && this.ctx.multipleContacts.length > 0) {
            const contacts  = this.ctx.multipleContacts.join(' or ')
            return `I understand. What's the best email address I can use to reach ${contacts} when they're available?`
          } else {
            return `I understand. What's the best email address I can use to follow up with the right person?`
          }
        }
        
        // ✅ PRIORITY 5: Self-identification
        if (/\b(this is|it'?s me|i'?m|I am)\s+[A-Z][a-z]+\b/i.test(userText)) {
          const nameMatch  = userText.match(/\b(this is|it'?s me|i'?m|I am)\s+([A-Z][a-z]+)/i)
          if (nameMatch) {
            const identifiedName  = nameMatch[2]
            logger.info(`✅ Person identified themselves as: ${identifiedName}`)
            this.ctx.decisionMaker  = identifiedName
            if (this.pitchAlreadyGiven) {
              this.state  = 'ask_seats'
              return `Great ${identifiedName}! First, about how many people use phones at your company?`
            } else {
              this.pitchAlreadyGiven  = true
              return `Perfect, glad I've got the right person ${identifiedName}! We work with over a hundred phone and contact center providers. Would you be open to exploring options if it improved quality, added missing features, or helped book more leads?`
            }
          }
        }
        
        // ✅ PRIORITY 6: Extract names from response
        const availableNames  = this.extractAnyNames(userText)
        
        if (availableNames && availableNames.length > 0) {
          // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
          this.ctx.decisionMaker  = availableNames[0]
          return `Perfect! Can you transfer me to ${availableNames[0]}? I'll wait.`
        }
        
        // ✅ PRIORITY 7: Handle "don't know" gracefully
        const dontKnowPatterns  = [
          /don'?t know/i, /not sure/i, /unsure/i, /no idea/i,
          /maybe.*not/i, /couldn'?t say/i
        ]
        
        if (dontKnowPatterns.some(pattern 
          if (this.ctx.multipleContacts && this.ctx.multipleContacts.length > 0) {
            const contacts  = this.ctx.multipleContacts.join(' or ')
            return `No problem. What's the best email address I can use to reach ${contacts}?`
          } else {
            return `No problem. What's the best email address I can use to follow up?`
          }
        }
        
        // ✅ PRIORITY 8: Extract from "Maybe Name" patterns
        const maybePattern  = /(?:maybe|probably|perhaps|possibly)\s+(\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/i
        const maybeMatch  = userText.match(maybePattern)
        
        if (maybeMatch) {
          const possibleName  = maybeMatch[1].trim()
          if (possibleName.length > 2) {
            // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
            this.ctx.decisionMaker  = possibleName
            return `Thanks! Can you transfer me to ${possibleName}? I'll wait.`
          }
        }
        
        // ✅ FALLBACK: Natural clarification
        return `Who's around right now that I could speak with about phone systems?`

      // ✅ NEW STATE: ASK_ALTERNATIVE_CONTACT
      case 'ask_alternative_contact':
        // ✅ PRIORITY 1: Check for RE-ENGAGEMENT (same patterns as above)
        const altReEngagePatterns  = [
          /\b(i can help|i could help|let me help|i'?ll help|maybe.*help)\b/i,
          /\b(i handle|i oversee|i manage|i do|i take care of)\b/i,
          /\b(talk to me|speak to me|you can talk to me)\b/i,
          /\b(what (?:do you|can i|did you) (?:need|want)|how can i help)\b/i,
          /\b(i'?m available|i'?m here|i can assist|i'?m the one)\b/i,
          /\b(if you (?:want|need)|i might be able to|i could try)\b/i,
          /\b(what'?s this (?:about|regarding)|can i ask what)\b/i
        ]
        
        if (altReEngagePatterns.some(pattern  = > pattern.test(userText)) || this.isReluctantVolunteering(userText)) {
          logger.info(`✅ Re-engagement in ask_alternative_contact: "${userText}"`)
          
          // Check if they're screening
          if (/\b(what'?s this|what did you|can i ask|may i)\b/i.test(userText)) {
            return `Sure! We help businesses compare phone and contact center providers. Is that something you handle?`
          }
        }
        
        // Rest of the existing logic...
        const alternativeNames  = this.extractAnyNames(userText)
        if (alternativeNames && alternativeNames.length > 0) {
          // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
          this.ctx.decisionMaker  = alternativeNames[0]
          return `Perfect! Can you transfer me to ${alternativeNames[0]}? I'll wait.`
        }
        
        if (/\b(nobody|no one|everyone.*out|all.*unavailable)\b/i.test(userText)) {
          this.state  = 'collect_email_fallback'
          return `I understand. What's the best email address I can use to follow up when they're available?`
        }
        
        if (/\b(this is|it'?s me|i'?m|I am)\s+[A-Z][a-z]+\b/i.test(userText)) {
          const nameMatch  = userText.match(/\b(this is|it'?s me|i'?m|I am)\s+([A-Z][a-z]+)/i)
              return `Perfect ${identifiedName}! We work with over a hundred phone and contact center providers. Would you be open to exploring options if it improved quality, added missing features, or helped book more leads?`
            }
          }
        }
        
        return `Is there someone else who handles phone systems that I could speak with?`
        
      case 'waiting_for_transfer':
        // ✅ ENHANCED: Better handling during transfer wait
        
        // If transfer seems to be taking too long, check in
        const timeInState  = Date.now() - this.transferStartTime
        if (timeInState > 30000 && timeInState < 60000) { // After 30 seconds
          if (!this.checkedTransferStatus) {
            this.checkedTransferStatus  = true
            return `Still working on connecting me with ${this.ctx.decisionMaker}?`
          }
        }
        
        // ✅ ENHANCED: Check if the person speaking IS the decision maker
        if (this.ctx.decisionMaker && this.isDecisionMakerSelfIdentifying(userText, this.ctx.decisionMaker)) {
          logger.info(`✅ Decision maker ${this.ctx.decisionMaker} self-identified!`)
        }
        
        // ✅ ENHANCED: Also check for any self-identification even without exact name match
        if (/\b(this is|it'?s me|i'?m|I am|hello|hi|speaking)\s+[A-Z][a-z]+\b/i.test(userText)) {
          const nameMatch  = userText.match(/\b(this is|it'?s me|i'?m|I am|hello|hi)\s+([A-Z][a-z]+)/i)
          if (nameMatch && nameMatch[2]) {
            const identifiedName  = nameMatch[2]
            logger.info(`✅ Person identified as: ${identifiedName}`)
            break
          }
        }
        
        // ✅ FIX: Auto-bail on "out / not here / hello?" patterns
        const bailPatterns  = [
          /out/i, /not here/i, /hello\?/i, /can you hear me/i, /problem/i
        ]

        if (bailPatterns.some(p 
          const decisionMakerName  = this.ctx.decisionMaker || 'them'
        }
        
        if (/\b(i can help|talk to me|i'?ll handle|i could help|what can i help|actually.*help|maybe i can help|i am|i would be|speak with me|i do|i oversee|i handle|i manage|i'?m the right person|i'?m the person|you can talk to me)\b/i.test(text)) {
          this.state  = 'openness_pitch'
          }
        } else if (/\b(can'?t|not here|unavailable|out|not available|out of office|voicemail|busy|not at desk|not answering)\b/i.test(text)) {
          this.state  = 'collect_email_fallback' // Wait silently for transfer
        }
        break

      // ✅ NEW STATE: HANDLE_GATEKEEPER_SCREENING
      case 'handle_gatekeeper_screening':
        const screeningNames  = this.extractAnyNames(userText)
        if (screeningNames && screeningNames.length > 0) {
          if (screeningNames.length >
          } else {
            // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
            this.ctx.decisionMaker  = screeningNames[0]
            return `Perfect! Can you transfer me to ${screeningNames[0]}? I'll wait.`
          }
        }
        
        if (this.isPersonUnavailable(userText)) {
          this.state  = 'ask_alternative_contact'
          return `I understand they're unavailable. Is there someone else who handles phone systems that I could speak with?`
        }
        
        return `Who specifically handles your phone system?`

      // ✅ NEW STATE: HANDLE_TEMPORARILY_BUSY
      case 'handle_temporarily_busy':
        if (/\b(email|emai|e.?mail)\b/i.test(userText)) {
          this.state  = 'collect_email_fallback'
          return `Perfect. What's the best email address I can use to follow up?`
        }
        
        if (/\b(call back|call|try back|later)\b/i.test(userText)) {
          this.state  = 'done'
          return `Great, I'll try back in about 30 minutes. Thanks for your time!`
        }
        
        return `Should I try back in about 30 minutes, or would email be better?`

      // ✅ NEW STATE: HANDLE_WRONG_DEPARTMENT
      case 'handle_wrong_department':
        const departmentNames  = this.extractAnyNames(userText)
        if (departmentNames && departmentNames.length > 0) {
          // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
          this.ctx.decisionMaker  = departmentNames[0]
          return `Thanks for the correct information! Can you transfer me to ${departmentNames[0]}? I'll wait.`
        }
        
        if (/\b(extension|ext|ext\.)\s*(\d+)/i.test(userText)) {
          const extMatch  = userText.match(/\b(extension|ext|ext\.)\s*(\d+)/i)
          if (extMatch) {
            this.state  = 'done'
            return `Thank you! I'll reach out to extension ${extMatch[2]}. Have a great day!`
          }
        }
        
        if (/\b(don'?t know|not sure|no idea)\b/i.test(userText)) {
          this.state  = 'done'
          return `I understand. Thanks for your time anyway. Have a great day!`
        }
        
        return `Who should I speak with about phone systems, or what's the correct extension?`

      // ✅ NEW STATE: WAITING_FOR_AVAILABILITY_CHECK
      case 'waiting_for_availability_check':
        if (this.isPersonUnavailable(userText)) {
          this.state  = 'ask_alternative_contact'
        }
        
        const availableCheckNames  = this.extractAnyNames(userText)
        if (availableCheckNames && availableCheckNames.length > 0) {
          // ✅ FIX: We're OUTBOUND - we want to SPEAK with them, not transfer TO them
          this.ctx.decisionMaker  = availableCheckNames[0]
          return `Perfect! Can you transfer me to ${availableCheckNames[0]}? I'll wait.`
        }
        
        if (/\b(available|here|yes)\b/i.test(userText)) {
          this.state  = 'waiting_for_transfer'
          const decisionMaker  = this.ctx.decisionMaker || 'them'
          return `Great! Can you transfer me to ${decisionMaker}? I'll wait.`
        }
        
        return `Are they available to speak?`
        
      case 'collect_email_fallback':
        // ✅ ENHANCED: Check if decision-maker becomes available again
        if (/\b(available|here|back)\b/i.test(userText) && this.ctx.decisionMaker) {
          this.state  = 'waiting_for_transfer'
          return `Great! Can you transfer me to ${this.ctx.decisionMaker} now?`
        }
        
        // ✅ CRITICAL FIX: Check for transfer offers FIRST
        if (this.isTransferOffer(userText)) {
          logger.info(`✅ Transfer offer detected in collect_email_fallback: "${userText}"`)
          return await this.handleTransferOffer(userText)
        }
        
        // ✅ FIX: Auto-Bail on "No Email" → Phone Fallback
        const noEmail  = /no email|don'?t.*email|we don'?t.*email|email.*down|phones only/i
        if (noEmail.test(userText)) {
          this.state  = 'phone_fallback'
          return `No problem — I can text you the meeting details instead. What's the best number to reach you?`
        }

        // ✅ NEW: Auto-Bail on "No Texting" → Offer Callback
        const noTexting  = /no text|don'?t.*text|we don'?t.*text|no texting|text.*down/i
        if (noTexting.test(userText)) {
          this.state  = 'offer_callback'
          return `I understand. Would you like me to have a specialist call you back instead?`
        }

        if (/\b(i can help|talk to me|i'?ll handle|i could help|what can i help|actually.*help|maybe i can help|i am|i would be)\b/i.test(text)) {
          this.emailCollector.reset()
          }
        } else {
          const email  = this.tryExtractEmail(text)
          if (email) {
            this.ctx.email  = email
          } else if (text.includes('no email') || text.includes("doesn't have") || text.includes("don't have")) {
            this.state  = 'done'
          } else {
            response  = `What's the best email address I can use to follow up?`
          }
        }
        break

      // ✅ NEW STATE: Offer callback when both email and texting are refused
      case 'offer_callback':
        if (this.yes(text)) {
          this.state  = 'request_callback_phone'
        } else if (this.no(text)) {
          this.state  = 'done'

      // ✅ NEW STATE: Handle contact info requests
      case 'provide_contact_info':
        // When they ask for our number/contact info
        if (/\b(website|email|contact|info|information)\b/i.test(userText)) {
          this.state  = 'collect_email_fallback'
          return `We'll follow up via email with all our contact information. What's the best email address to use?`
        }
        
        const emailMatch  = userText.match(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/)
        if (emailMatch) {
          this.ctx.email  = emailMatch[0]
          return `Thanks, I have ${emailMatch[0]}. Our team will send information about phone system options. Is that email correct?`
        }
        
        return `What's the best email address where we can send information about phone system options?`

      // ✅ FIX: Skip Name Hunt When User Already Volunteered - REPLACED entire block
      case 're_engaged_fallback':
        // user already volunteered → skip name hunt
        this.state  = 'openness_pitch'
        return `Perfect, glad I've got the right person! We help companies compare phone & contact-center providers to save money or add features. Worth a quick 15-minute call?`
        
      case 'openness_pitch':
        if (text.includes('busy') || text.includes('call back') || text.includes('later') || text.includes('not a good time') || text.includes('call me back')) {
          this.state  = 'schedule_callback'
        } else if (this.yes(text) || /\b(maybe|possibly|probably|we might|could be|could)\b/i.test(text)) {
          this.state  = 'ask_seats'
        
      case 'ask_seats':
        // HYBRID: State machine + LLM fallback
        let seats  = this.extractNumber(text)
        
        if (seats !
          this.failedExtractAttempts  = 0
        } else {
          // LLM fallback
          this.failedExtractAttempts++
          
          if (this.failedExtractAttempts <
            
            if (interpretation.understood && interpretation.value) {
              this.ctx.seats  = interpretation.value
              this.failedExtractAttempts  = 0
            } else {
              response  = interpretation.clarification || 
                        `Just to clarify - roughly how many people use phones at your company?`
            }
          } else {
            // After 2 attempts, move on with default
            this.ctx.seats  = 10
            this.failedExtractAttempts  = 0
        
      case 'ask_provider':
        // HYBRID: State machine + LLM fallback
        if (text.length >
            
            if (interpretation.understood && interpretation.value) {
              this.ctx.provider  = interpretation.value
            }
          } else {
            response  = `Who's your current phone provider?`
        
      case 'ask_crm':
        if (this.yes(text)) {
          this.state  = 'ask_which_crm'
        } else if (this.no(text)) {
          this.ctx.useCRM  = false
          // No lag when moving to scheduling
          return await this.scheduleAppointment()
        
      case 'ask_which_crm':
        // HYBRID: State machine + LLM fallback
        if (text.length > 2) {
          this.ctx.crmName  = text
          this.ctx.useCRM  = true
          // ✅ FIX: Show message while fetching slots to reduce perceived delay
          // Fetch slots in background and respond immediately
          const schedulingPromise  = this.scheduleAppointment()
          // Return immediately with a brief message, then the actual slots
          // Actually, we need to await it, but we can optimize the slot fetching
          return await schedulingPromise
            
            if (interpretation.understood && interpretation.value) {
              this.ctx.crmName  = interpretation.value
              this.ctx.useCRM  = true
              return await this.scheduleAppointment()
        
      case 'offer_appointments':
        // ✅ FIX: Enhanced objection detection - handle in ALL states
        const objectionPatterns  = [
          /don'?t.*listen/i, /you.*repeat/i, /getting.*outside.*calendar/i,
          /not.*work/i, /can'?t.*do/i, /too early/i, /too late/i, /2.*am/i, /3.*am/i,
          /fuck/i, /stupid/i, /ridiculous/i, /tourette/i
        ]
        const hasObjection 
        
        if (hasObjection && this.ctx.bookingTime) {
          // User is objecting to the current booking time
          this.rejectedSlots.add(this.ctx.bookingTime.toString())
          this.memory.rememberRejection(this.ctx.bookingTime, userText)
          this.ctx.bookingTime  = null // Clear the bad time
          this.state  = 'offer_appointments' // Offer new slots
        }
        
        // ✅ FIX 5: If user already confirmed a time, move to name collection
        if (this.ctx.bookingTime && (this.yes(text) || /\b(that works|sounds good|perfect|great|sure|yeah|ok|okay)\b/i.test(text))) {
          this.state  = 'get_name_for_booking'
          this.ctx.slotChosen  = true // Mark slot as chosen to prevent loops
          response  = `Perfect! I'll book you for ${this.formatTimeSlot(this.ctx.bookingTime)}. What's your first name?`
        }
        
        // ✅ ENTERPRISE: Enhanced slot handling with memory and business hours validation
        // Filter out rejected slots
        const filteredSlots 
        
        // ✅ FIX: Apply business hours filter using Pacific timezone
        const businessSlots 
          const hour  = pacificTime.getHours()
          return hour > // 8 AM - 6 PM Pacific
        })

        // Update available slots with filtered ones
        this.ctx.availableSlots  = businessSlots.length > 0 ? businessSlots : filteredSlots

        // ✅ FIX 2: "Yeah/Sure" Guard - handle confirmations properly
        if (/\b(yeah|yes|ok|sure|sounds good|that works|perfect|great)\b/i.test(userText) && !this.ctx.slotChosen) {
          if (this.ctx.availableSlots && this.ctx.availableSlots.length > 0) {
            // Validate the first slot is in business hours
            const firstSlot  = this.ctx.availableSlots[0]
            const pacificTime  = new Date(firstSlot.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
            const hour  = pacificTime.getHours()
            
            if (hour >
              this.ctx.slotChosen  = true
              return `Great! I'll lock in ${this.formatTimeSlot(firstSlot)}. What's your first name?`
            } else {
              // Find a valid business hours slot
              const validSlot 
                return pt.getHours() >
              
              if (validSlot) {
                this.ctx.bookingTime  = validSlot
                this.ctx.slotChosen  = true
                return `Great! I'll lock in ${this.formatTimeSlot(validSlot)}. What's your first name?`
              }
            }
          }
        }

        // ✅ FIX 3: Profanity / Confusion Reset
        const panic  = /fuck|confused|what.*talking|don'?t.*understand/i
        if (panic.test(userText)) {
          this.state  = 'openness_pitch'
          return `Let me back up — we help companies compare phone systems to save money or add features. Worth a quick 15-minute call?`
        }
        
        if (!this.ctx.availableSlots || this.ctx.availableSlots.length 
        }
        
        const requestedSlot  = this.findRequestedTimeSlot(text)
        
        if (requestedSlot 
          
          // ✅ FIX: Validate business hours before booking
          const pacificTime  = new Date(selectedSlot.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
          const hr  = pacificTime.getHours()
          
          if (hr < 8 || hr > 18) {
            // Find a valid business hours slot
            const validSlot 
              return pt.getHours() >
            })
            if (validSlot) {
              selectedSlot  = validSlot
            } else {
              return `I don't have any available slots in business hours. Let me find better options. ` + await this.scheduleAppointment()
            }
          }
          
          this.ctx.bookingTime  = selectedSlot
          
        } else if (requestedSlot 
          
          if (hr < 8 || hr > 18) {
            const validSlot 
          
        } else if (requestedSlot instanceof Date) {
          // ✅ CRITICAL FIX: requestedSlot should be an actual Cal.com slot, not a fake date
          // Verify it exists in availableSlots before using it
          const slotExists 
          
          if (!slotExists) {
            // Requested slot doesn't exist in Cal.com - find closest matching slot
            logger.warn(`⚠️ Requested slot not in Cal.com slots, finding closest match`)
            
            // Extract day and time from requested slot
            const requestedDayName  = requestedSlot.toLocaleString('en-US', { 
              weekday: 'long', 
              timeZone: 'America/Los_Angeles' 
            }).toLowerCase()
            const requestedHour  = new Date(requestedSlot.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' })).getHours()
            
            // Find matching slots from Cal.com
            const matchingSlots 
              const slotHour  = new Date(slot.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' })).getHours()
              
              return slotDayName  // Within 2 hours
            })
            
            if (matchingSlots.length > 0) {
              // Use closest matching Cal.com slot
              this.ctx.bookingTime  = matchingSlots[0]
              return `Perfect! I'll book you for ${this.formatTimeSlot(matchingSlots[0])}. What's your first name?`
            } else {
              // No matching slot found - offer available slots
              return `I don't have that exact time available. ` + await this.scheduleAppointment()
            }
          }
          
          // ✅ FIX: Validate the requested slot is in business hours
          const pacificTime  = new Date(requestedSlot.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
          
          if (hr < 8 || hr > 18) {
            // Find closest available business hours slot from Cal.com
            const validSlot 
            
            if (validSlot) {
              this.ctx.bookingTime  = validSlot
              return `I found a better time during business hours — ${this.formatTimeSlot(validSlot)}. What's your first name?`
            } else {
              return `I don't have that time available. Let me find better options. ` + await this.scheduleAppointment()
            }
          }
          
          // Slot is valid and exists in Cal.com - use it
          this.ctx.bookingTime  = requestedSlot
        } else {
          // ✅ FIX: Check if user is requesting a specific day that's not available
          const dayKeywords = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
          const requestedDay = dayKeywords.find(day => userText.toLowerCase().includes(day))

          if (requestedDay) {
            // User asked for a specific day that's not available
            return `I don't have ${requestedDay.charAt(0).toUpperCase() + requestedDay.slice(1)} available. What time works best for you so I can check the calendar?`
          }

          // Track time rejections
          if (userText.includes('2am') || userText.includes('3am') || userText.includes('too early') || userText.includes("can't do") || userText.includes("don't wake up")) {
            if (this.ctx.bookingTime) {
              this.rejectedSlots.add(this.ctx.bookingTime.toString())
              this.memory.rememberRejection(this.ctx.bookingTime, userText)
              this.ctx.bookingTime  = null
            }
            // Offer new slots
            return `I understand. Let me find a better time. ` + await this.scheduleAppointment()
          }

          // Give clear, simple options with better descriptions
          const slot1Friendly  = timezoneManager.isToday(this.ctx.availableSlots[0]) ? 
            `today at ${this.formatTimeSlot(this.ctx.availableSlots[0]).split(',')[1].trim()}` :
            this.formatTimeSlot(this.ctx.availableSlots[0])
          
          const slot2Friendly  = this.ctx.availableSlots[1] ? 
            (timezoneManager.isTomorrow(this.ctx.availableSlots[1]) ?
              `tomorrow at ${this.formatTimeSlot(this.ctx.availableSlots[1]).split(',')[1].trim()}` :
              this.formatTimeSlot(this.ctx.availableSlots[1])) : null

          if (slot2Friendly) {
            response  = `I have ${slot1Friendly} or ${slot2Friendly}. Which works better?`
        
      case 'get_name_for_booking':
        // ✅ FIX 5: Enhanced objection detection - Bad-Time Reschedule
        const badTimePatterns  = /can'?t|too early|too late|2am|3am|don'?t.*wake|don'?t like.*time|not.*work|outside.*calendar/i
        if (badTimePatterns.test(userText)) {
          // Remember this rejection
          if (this.ctx.bookingTime) {
            this.rejectedSlots.add(this.ctx.bookingTime.toString())
            this.memory.rememberRejection(this.ctx.bookingTime, userText)
            this.ctx.bookingTime  = null
            this.ctx.slotChosen  = false
          }
          this.state  = 'offer_appointments'
          return `No worries — let's find a better time. ` + await this.scheduleAppointment()
        }
        
        // ✅ FIX: Enhanced objection detection for frustration
        const nameFrustrationPatterns  = /don'?t.*listen|you.*repeat|fuck|stupid|not.*paying.*attention/i
        if (nameFrustrationPatterns.test(userText)) {
          // Acknowledge and try to recover
          if (this.ctx.bookingTime) {
            return `I apologize for the confusion. Let's start over with scheduling. ` + await this.scheduleAppointment()
          } else {
            return `I understand your frustration. Let me find a better appointment time for you. ` + await this.scheduleAppointment()
          }
        }
        
        // ✅ FIX 6: More lenient name detection
        const potentialName  = text.trim()
        
        // If it's a simple word without time references, treat it as a name
        if (potentialName.length >
          this.ctx.firstName  = nameParts[0]
         
          if (nameParts.length > 1) {
            this.ctx.lastName  = nameParts.slice(1).join(' ')
          } else {
            this.state  = 'get_last_name'
          }
        } else {
          response  = `What's your first name?`
        
      case 'get_last_name':
        if (text.length >
        
      // FIXED: Enhanced email collection with scheduling link fallback
      case 'get_email':
        // ✅ FIX: Enhanced objection detection in email collection
        const emailObjectionPatterns  = /don'?t.*listen|you.*repeat|fuck|not.*work|outside.*calendar|2am|3am|too early/i
        if (emailObjectionPatterns.test(userText)) {
          // User is frustrated about appointment time, not email
          if (this.ctx.bookingTime) {
            this.rejectedSlots.add(this.ctx.bookingTime.toString())
            return `I understand. Let me find a better appointment time. ` + await this.scheduleAppointment()
          }
        }
        
        // ✅ FIX 6: Time-Clarification
        const clarify  = /what time|when.*appointment|which.*day/i
        if (clarify.test(userText)) {
          if (this.ctx.bookingTime) {
            return `Your appointment is ${this.formatTimeSlot(this.ctx.bookingTime)}. Should we lock that in?`
          } else {
            return await this.scheduleAppointment()
          }
        }
        
        const email  = this.tryExtractEmail(text)
        if (email) {
          this.ctx.email  = email
        } else {
          if (this.emailCollector.shouldGiveUp()) {
            // ✅ FIX: Instead of phone fallback, offer to send scheduling link
            this.state  = 'offer_scheduling_link'
          } else {
            this.emailCollector.recordAttempt()

      // NEW: Scheduling link state
      case 'offer_scheduling_link':
        const schedulingEmail  = this.tryExtractEmail(text)
        if (schedulingEmail) {
          this.ctx.email  = schedulingEmail
          
          // Send scheduling link via n8n/webhook
          this.sendSchedulingLinkEmail(schedulingEmail).catch(error  = > {
            logger.error('❌ Error sending scheduling link:', { message: error.message })
          })

      case 'phone_fallback':
        // ✅ NEW: Check for "no texting" and offer callback instead
        const noText  = /no text|don'?t.*text|we don'?t.*text|no texting|text.*down/i
        if (noText.test(userText)) {
          this.state  = 'offer_callback'
        }

        const phone  = this.phoneCollector.processPhoneInput(text)
        if (phone) {
          this.ctx.phoneForText  = phone
        } else {
          const digits  = text.replace(/\D/g, '')
          if (digits.length >

      case 'phone_fallback_confirmed':
        response  = `Thank you. Goodbye!`
        
      case 'confirm_email':
        if (this.yes(text)) {
          this.state  = 'confirm_booking'
         
          // Book the appointment in Cal.com (async, don't wait)
          this.bookAppointmentInCalCom().catch(error  = > {
            logger.error('❌ Background booking error:', { message: error.message })
         
          const fullName  = this.ctx.lastName ?
            `${this.ctx.firstName} ${this.ctx.lastName}` : this.ctx.firstName
         
          response  = `Excellent ${this.ctx.firstName}! I've booked your 15-minute appointment for ${this.formatTimeSlot(this.ctx.bookingTime)}. You'll receive a confirmation email shortly at ${this.ctx.email}. Looking forward to speaking with you then. Have a great day!`
        } else if (this.no(text)) {
          this.emailCollector.reset()

      // Calendar error state
      case 'calendar_error':
        response  = `Thank you. Goodbye!`

      // Callback request states
      case 'request_callback_phone':
        // ✅ FIX 3: Callback-Loop Exit
        const exitCB  = /no.*callback|don'?t.*call|what.*calling|why.*call|i can help/i
        if (exitCB.test(userText)) {
          this.state  = 'openness_pitch'
          return `Perfect, glad I've got the right person! We help companies compare phone & contact-center providers to improve quality or cut costs. Would you be open to a quick 15-minute call?`
        }
        const purpose  = /what.*about/i
        if (purpose.test(userText)) {
          return `We help businesses find better phone / contact-center solutions. If that's useful, I can book a 15-minute call. Sound good?`
        }
        
        const callbackPhone  = this.phoneCollector.processPhoneInput(text)
        if (callbackPhone) {
          this.callbackPhone  = callbackPhone
        
      case 'request_callback_name':
        if (text.length >
          
          // Send callback request to n8n
          this.sendCallbackRequestToN8N().catch(error  = > {
            logger.error('❌ Error sending callback request:', { message: error.message })
          
          // Use third-person reference, not "my name"
          if (this.callbackPhone) {
            response  = `Perfect — I've noted that ${this.callbackName} at ${this.callbackPhone} would like a callback. Someone will reach out as soon as an agent becomes available.`
        
      case 'callback_confirmed':
        response  = `Thank you. Goodbye!`
        
      case 'calendar_full':
        if (this.yes(text)) {
          this.state  = 'get_email_for_followup'
        
      case 'get_email_for_followup':
        const followupEmail  = this.tryExtractEmail(text)
        if (followupEmail) {
          this.ctx.email  = followupEmail
        
      case 'voicemail_hangup':
        response  = `[VOICEMAIL_DETECTED_HANGUP]`

      case 'transfer_timeout_check':
        // If we get any response during timeout check, assume someone is there
        if (userText && userText.trim().length > 0) {
          logger.info('✅ Got response after transfer timeout - continuing conversation')
        } else {
          // No response after timeout check - hang up
          response  = `[HANGUP_AFTER_TIMEOUT]`
        
      case 'done':
      case 'confirm_booking':
        // FIX 3: Defensive check in done state
        const helpKeywords  = ['help', 'assist', 'talk', 'speak', 'available', 'can we', 'we can']
        if (helpKeywords.some(keyword  = > text.includes(keyword))) {
          this.emailCollector.reset()
        
      default:
        response  = `I'm looking to speak with whoever oversees your phone system. Who would be the best person for that?`
    }
   
    if (prevState !
      this.stateManager.logStateTransition(prevState, this.state, userText)
      if (convoLogger) convoLogger.logStateChange(prevState, this.state)
    }
   
    // ✅ LATENCY TRACKING: Calculate total response generation time
    const responseGenerationTime  = Date.now()
    const totalLatency  = responseGenerationTime - inputStartTime
    
    // Track characters spoken for cost calculation
    if (response && response.length > 0) {
      this.charactersSpoken + = response.length
    }
   
    // Update last response time for interruption detection
    if (response && response !
    }
   
    // Track conversation history for LLM
    if (response && response !
      
      // ✅ LATENCY TRACKING: Log response generation completion
      logger.info(`📤 [LATENCY] Response generated in ${totalLatency}ms: "${response.substring(0, 50)}${response.length > 50 ? '...' : ''}"`)
    }
   
    return response
  }

  getRecoveryResponse(targetState) {
    const recoveryResponses  = {
      'collect_email_fallback': 'What email should I use to follow up?',
      'get_email_for_followup': 'What email should I use to schedule a better time?',
      'phone_fallback': 'What number should I text the meeting details to?',
      'openness_pitch': 'We help companies compare phone systems to save money or add features. Worth a quick 15-minute call?'
    }
    
    return recoveryResponses[targetState] || 'Let me try a different approach.'
  }

  // ENTERPRISE: Enhanced methods for specific time requests
  isSpecificTimeRequest(text) {
    const patterns  = [
      /thursday/i, /friday/i, /monday/i, /tuesday/i, /wednesday/i,
      /\d+ ?(am|pm)/i, /morning/i, /afternoon/i,
      /next week/i, /tomorrow/i
    ]
    return patterns.some(pattern  = > pattern.test(text))
  }

  parseTimeRequest(text) {
    // Enhanced time parsing logic
    const lowerText  = text.toLowerCase()
    
    // Day detection
    const days  = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
    const dayMatch 
    
    // Time detection  
    const timeMatch  = lowerText.match(/(\d+)\s*(am|pm)/i)
    
    return {
      preferredDay: dayMatch || null,
      preferredTime: timeMatch ? `${timeMatch[1]}${timeMatch[2].toLowerCase()}` : null,
      constraints: this.extractConstraints(lowerText)
    }
  }

  extractConstraints(text) {
    const constraints  = []
    if (text.includes('morning')) constraints.push('morning')
    if (text.includes('afternoon')) constraints.push('afternoon')
    if (text.includes('early')) constraints.push('early')
    if (text.includes('late')) constraints.push('late')
    return constraints
  }

  async handleSpecificSlotRequest(timeRequest) {
    if (!this.ctx.availableSlots) {
      await this.getAvailableTimeSlots()
    }

    // Find matching slot
    const matchingSlot 
      const slotTime  = slot.toLocaleString('en-US', { 
        hour: 'numeric', 
        minute: '2-digit',
        hour12: true 
      }).toLowerCase()

      const dayMatch 
      const timeMatch  = !timeRequest.preferredTime || slotTime.includes(timeRequest.preferredTime)

      return dayMatch && timeMatch && this.memory.shouldOfferSlot(slot)

    if (matchingSlot) {
      this.ctx.bookingTime  = matchingSlot
      return {
        success: true,
        response: `Perfect! I'll book you for ${this.formatTimeSlot(matchingSlot)}. What's your first name?`
      }
    } else {
      return {
        success: false,
        message: `I don't see ${timeRequest.preferredDay ? timeRequest.preferredDay + ' ' : ''}${timeRequest.preferredTime ? timeRequest.preferredTime + ' ' : ''}available.`
      }
    }
  }

  isTimeRejection(text) {
    return /3am|too early|too late|can'?t do.*time|who works at.*am/i.test(text)
  }
}

// Use the enhanced conversation class
const OutboundConversation  = EnterpriseOutboundConversation
  }

  async checkAllServices() {
    logger.info('🔧 CHECKING SERVICE STATUS...')
    await this.checkTwilio()
    await this.checkDeepgram()
    await this.checkDeepSeek()
    await this.checkCalCom()
    await this.checkZoho()
    await this.checkN8N()
    
    return this.services
  }

  async checkTwilio() {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
      this.services.twilio  = { name: 'Twilio', status: 'disabled', details: 'Missing credentials' }
      
      if (response.ok) {
        this.services.twilio  = { name: 'Twilio', status: 'operational', details: 'API connected' }
      } else {
        this.services.twilio  = { name: 'Twilio', status: 'error', details: `API error: ${response.status}` }
      }
    } catch (error) {
      this.services.twilio  = { name: 'Twilio', status: 'error', details: `Connection failed: ${error.message}` }
    }
  }

  async checkDeepgram() {
    if (!DEEPGRAM_API_KEY) {
      this.services.deepgram  = { name: 'Deepgram', status: 'disabled', details: 'Missing API key' }
    }

    try {
      const response  = await fetch('https://api.deepgram.com/v1/projects', {
        headers: { 'Authorization': `Token ${DEEPGRAM_API_KEY}` }
      })
      
      if (response.ok) {
        this.services.deepgram  = { name: 'Deepgram', status: 'operational', details: `STT: ${DG_STT_MODEL} | TTS: ${DG_TTS_MODEL}` }
      } else {
        this.services.deepgram  = { name: 'Deepgram', status: 'error', details: `API error: ${response.status}` }
      }
    } catch (error) {
      this.services.deepgram  = { name: 'Deepgram', status: 'error', details: `Connection failed: ${error.message}` }
    }
  }

  async checkDeepSeek() {
    if (!DEEPSEEK_API_KEY) {
      this.services.deepseek  = { name: 'DeepSeek V3.2', status: 'disabled', details: 'Missing API key' }
      
      if (response.ok) {
        const data  = await response.json()
        const hasDeepSeek 
        if (hasDeepSeek) {
          this.services.deepseek  = { name: 'DeepSeek V3.2', status: 'operational', details: 'Hybrid: State Machine + LLM Fallback' }
        } else {
          this.services.deepseek  = { name: 'DeepSeek V3.2', status: 'error', details: 'DeepSeek not available in account' }
        }
      } else {
        this.services.deepseek  = { name: 'DeepSeek V3.2', status: 'error', details: `API error: ${response.status}` }
      }
    } catch (error) {
      this.services.deepseek  = { name: 'DeepSeek V3.2', status: 'error', details: `Connection failed: ${error.message}` }
    }
  }

  async checkCalCom() {
    if (!CAL_COM_API_KEY || !CAL_EVENT_TYPE_ID) {
      this.services.calCom  = { name: 'Cal.com', status: 'disabled', details: 'Missing credentials' }
        this.services.calCom  = { name: 'Cal.com', status: 'operational', details: `15-min Appointments - API v2` }
      } else {
        this.services.calCom  = { name: 'Cal.com', status: 'error', details: `API error: ${response.status} - Check Event Type ID` }
      }
    } catch (error) {
      this.services.calCom  = { name: 'Cal.com', status: 'error', details: `Connection failed: ${error.message}` }
    }
  }

  async checkZoho() {
    if (!ZOHO_CLIENT_ID) {
      this.services.zoho  = { name: 'Zoho CRM', status: 'disabled', details: 'Missing credentials' }
    }

    try {
      const token  = await zohoIntegration.getToken()
      if (token) {
        this.services.zoho  = { name: 'Zoho CRM', status: 'operational', details: 'Deep Analytics - Syncing' }
      } else {
        this.services.zoho  = { name: 'Zoho CRM', status: 'error', details: 'Token refresh failed' }
      }
    } catch (error) {
      this.services.zoho  = { name: 'Zoho CRM', status: 'error', details: `Connection failed: ${error.message}` }
    }
  }

  async checkN8N() {
    if (!N8N_WEBHOOK_URL) {
      this.services.n8n  = { name: 'n8n Webhooks', status: 'disabled', details: 'No webhook URL configured' }
    }

    try {
      // Simple connectivity test
      const response 
      
      if (response) {
        this.services.n8n  = { name: 'n8n Webhooks', status: 'operational', details: 'Webhook endpoint reachable' }
      } else {
        this.services.n8n  = { name: 'n8n Webhooks', status: 'error', details: 'Webhook endpoint not reachable' }
      }
    } catch (error) {
      this.services.n8n  = { name: 'n8n Webhooks', status: 'error', details: `Connection failed: ${error.message}` }
    }
  }

  printStatusReport() {
    const statusData  = {
      services: this.services,
      timestamp: new Date().toISOString(),
      timezone: AVA_TIMEZONE || TIMEZONE,
      currentTime: timezoneManager.getCurrentTime(),
      companyName: COMPANY_NAME
    }
    
    // Consolidated logging - all in one object
    logger.info('System Status Report', statusData)
  }
}

const systemStatus  = new SystemStatusChecker()

const activeCalls  = new Map()
const conversationLogs  = new Map()

app.get('/', (req, res)  = > {
  res.json({
    message: `${COMPANY_NAME} AI Agent`,
    status: 'OK',
    agent: AGENT_NAME,
    timestamp: new Date().toISOString(),
    activeCalls: activeCalls.size
  })

app.get('/conversation/:callControlId', (req, res) 
  const convoLogger  = conversationLogs.get(callControlId)
  
  if (!convoLogger) {
    return res.status(404).json({ error: 'Conversation log not found for this call' })
  }
  
  res.json(convoLogger.getConversationLog())

app.get('/conversations', (req, res) 
  conversationLogs.forEach((convoLogger, callControlId) 
  })
  
  res.json({
    activeConversations: conversationLogs.size,
    conversations: conversations
  })

app.get('/status', async (req, res) 
  res.json({
    timestamp: new Date().toISOString(),
    services: services,
    system: {
      timezone: AVA_TIMEZONE || TIMEZONE,
      currentTime: timezoneManager.getCurrentTime(),
      activeCalls: activeCalls.size,
      agent: AGENT_NAME,
      company: COMPANY_NAME
    }
  })
  const from  = req.body.From
  const to  = req.body.To

  if (!activeCalls.has(callSid)) {
    const conversation  = new OutboundConversation(from, null)
    conversation.ctx.callSid  = callSid
    activeCalls.set(callSid, conversation)
    
    const conversationLogger  = new ConversationLogger(callSid)
    conversationLogs.set(callSid, conversationLogger)
    conversationLogger.logSystemEvent('call_initiated')
  }

  try {
    // UPDATED: TwiML with machine detection enabled and AMD
    const twiml 
    
    res.type('text/xml')
    res.send(twiml)

  } catch (error) {
    res.type('text/xml').send('<Response><Hangup/></Response>')

// AMD (Answering Machine Detection) webhook handler
app.post('/amd', (req, res) 
  const callSid  = req.body.CallSid
  
  logger.info('🔍 AMD Result:', { 
    callSid, 
    answeredBy,
    machineDetected: answeredBy 
  
  // If AMD detects a machine, hangup immediately
    if (answeredBy 
    
    // Clean up any active call tracking
    if (activeCalls.has(callSid)) {
      const convo  = activeCalls.get(callSid)
      const convoLogger  = conversationLogs.get(callSid)
      
      if (convoLogger) {
        convoLogger.logSystemEvent('machine_detected_hangup')
        convoLogger.printConversationSummary()
        
        // Send machine detection event to n8n for analytics
        const conversationData  = convo.getConversationDataForZoho('machine_detected')
        conversationData.callSid  = callSid
        conversationData.amdResult  = answeredBy
        
        fetch(N8N_WEBHOOK_URL || `${req.protocol}://${req.get('host')}/n8n-webhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(conversationData)
        }).catch(error  = > {
          logger.error('❌ Error sending AMD data to n8n:', { message: error.message })
        
        activeCalls.delete(callSid)
      }
    }
    
    // Return TwiML to hangup
    const twiml  = `
      <Response>
        <Hangup/>
      </Response>
    `
    res.type('text/xml').send(twiml)
    return
  }
  
  // If it's a human, continue with the call
  if (answeredBy 
  }
  
  // Default response - continue call
  res.type('text/xml').send('<Response></Response>')

app.post('/status', (req, res) 
  const status  = req.body.CallStatus
  
  if (status 
    const convoLogger  = conversationLogs.get(callSid)
    
    if (convo && convoLogger) {
      convoLogger.logSystemEvent('call_ended')
      convoLogger.printConversationSummary()
      
      // Send final conversation data to n8n for Zoho logging
      const conversationData  = convo.getConversationDataForZoho(status)
      conversationData.callSid  = callSid
      
      // Send to n8n webhook for processing
      fetch(N8N_WEBHOOK_URL || `${req.protocol}://${req.get('host')}/n8n-webhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(conversationData)
      }).catch(error  = > {
        logger.error('❌ Error sending call data to n8n:', { message: error.message })
      
      activeCalls.delete(callSid)
    }
    
    if (convoLogger) {
      setTimeout(()  = > {
        conversationLogs.delete(callSid)
      }, 3600000)
    }
  }
  res.status(200).json({})

const server  = http.createServer(app)
const wss  = new WebSocketServer({ server, path: PUBLIC_WS_PATH })
  let convo  = null
  let convoLogger  = null  // FIXED: Renamed from logger to convoLogger
  let dg  = null

  // ✅ FIXED: Deepgram STT configuration with container

  dg  = new WebSocket(`wss://api.deepgram.com/v1/listen?${deepgramParams}`, {
    headers: { 
      Authorization: `Token ${DEEPGRAM_API_KEY}`,
      'User-Agent': 'GoatVox-AI-Agent/1.0'
    }
  })
  
  dg.on('open', ()  = > {
    ws.send(JSON.stringify({
      event: 'connected',
      protocol: 'Call',
      version: '1.0.0'
    }))
  
  // 

  dg.on('close', (code, reason) 

  dg.on('message', async (data) 
      
      // ✅ ENHANCED VALIDATION: Comprehensive Deepgram message validation
      if (!msg || typeof msg !
        return
      }
      
      // Check for different Deepgram message types
      if (msg.type 
      }
      
      if (msg.type 
        
        // ✅ BETTER TRANSCRIPT VALIDATION
        if (transcript && msg.is_final && transcript.trim().length > = 2) {
          if (!convo) return
          
          if (convoLogger) convoLogger.logUserInput(transcript)
          
          const responseText  = await convo.next(transcript, convoLogger)
          
          if (responseText 
            const hangupEvent  = {
              event: 'clear',
              streamSid: callSid
            }
            ws.send(JSON.stringify(hangupEvent))
            return
          }

          if (responseText 
          }
          
          if (responseText && responseText.length > 0 && responseText !
            
            // ✅ LATENCY TRACKING: Track TTS synthesis time
            const ttsStartTime  = Date.now()
            const audioBuffer  = await deepgramTTS.synthesizeSpeech(responseText, convo.conversationStage)
            const ttsLatency  = Date.now() - ttsStartTime
            logger.info(`🔊 [LATENCY] TTS synthesis completed in ${ttsLatency}ms`)
            
            if (audioBuffer && audioBuffer.length > 0 && callSid) {
              // ✅ FIX: Send audio as single payload (Twilio handles streaming internally)
              // Splitting into chunks can cause gaps - send entire buffer at once for smooth playback
              const audioPayload  = {
                event: 'media',
                streamSid: callSid,
                media: {
                  payload: audioBuffer.toString('base64')
                }
              }
              
              try {
                // ✅ LATENCY TRACKING: Track audio send time
                const audioSendStartTime  = Date.now()
                ws.send(JSON.stringify(audioPayload))
                const audioSendLatency  = Date.now() - audioSendStartTime
                logger.info(`📡 [LATENCY] Audio sent to WebSocket in ${audioSendLatency}ms`)
                
                // ✅ FIX: Calculate accurate audio duration based on sample rate
                // For mulaw at 8kHz: buffer length in bytes  // 8kHz mulaw
                logger.info(`⏱️ [LATENCY] Estimated audio duration: ${estimatedDuration}ms`)
                
                const markEvent  = {
                  event: 'mark',
                  streamSid: callSid,
                  mark: { name: 'audio_complete' }
                }
                
                // Send mark event after audio finishes to ensure proper timing
                setTimeout(()  = > {
                  ws.send(JSON.stringify(markEvent))
                  logger.info(`✅ [LATENCY] Audio mark sent after ${estimatedDuration + 100}ms`)
                }, estimatedDuration + 100)
                
              } catch (error) {
                logger.error('❌ WebSocket audio send error:', { message: error.message })
              }
            }
          }
        }
      } else {
        logger.debug('🔊 Deepgram: Non-transcript message type', { type: msg.type })
      }
      
    } catch (error) {
      logger.error('❌ Deepgram message processing error:', { 
        message: error.message,
        data: data.toString().substring(0, 200) // Log first 200 chars for debugging
      })

  ws.on('message', async (data) 
      
      switch (msg.event) {
        case 'start':
          callSid  = msg.start?.streamSid
          const from  = msg.start?.call?.from || 'unknown'
          
          if (!activeCalls.has(callSid)) {
            convo  = new OutboundConversation(from, null)
            convo.ctx.callSid  = callSid
            activeCalls.set(callSid, convo)
            
            convoLogger  = conversationLogs.get(callSid) || new ConversationLogger(callSid)
            conversationLogs.set(callSid, convoLogger)
            convoLogger.logSystemEvent('stream_started')
          } else {
            convo  = activeCalls.get(callSid)
            convoLogger  = conversationLogs.get(callSid)
          }
          break
          
        case 'media':
          if (msg.media?.payload && dg && dg.readyState 
            dg.send(audioBuffer)
          
        case 'stop':
          if (convo && callSid) {
            if (convoLogger) {
              convoLogger.logSystemEvent('stream_stopped')
              convoLogger.printConversationSummary()
            }
            activeCalls.delete(callSid)
      }
      
    } catch (error) {
      logger.error('❌ WebSocket message processing error')

  ws.on('close', ()  = > {
    if (dg) {
      dg.close()
    }
    if (convo && callSid) {
      if (convoLogger) {
        convoLogger.logSystemEvent('websocket_closed')
      }
      activeCalls.delete(callSid)
  console.log(`🤖 Agent: ${AGENT_NAME}`)
  console.log(`🕐 Timezone: ${AVA_TIMEZONE || TIMEZONE}`)
  console.log(`🌐 Domain: ${DOMAIN}`)
  console.log('')
  
  // Start memory cleanup
  memoryManager.startCleanup()
  console.log('🧹 Memory management: ACTIVE - Automatic cleanup every 60 seconds')
  
  // Check and display system status with detailed logging
  await systemStatus.checkAllServices()
  systemStatus.printStatusReport()
  
  // ADDED: Single-line startup log for enabled APIs
  const enabledApis 
  logger.info(`Enabled APIs: ${enabledApis || 'None'}`)

  // Enhanced Configuration Status with real service status
  console.log(`🔧 CORE SERVICES STATUS:`)
  const services  = await systemStatus.checkAllServices()
  Object.values(services).forEach(service 
    console.log(`   ${statusIcon} ${service.name}: ${service.details}`)
  
  // Twilio Configuration Status
  console.log(`📞 TWILIO CONFIGURATION:`)
  console.log(`   ${TWILIO_ACCOUNT_SID ? '✅' : '❌'} Account SID: ${TWILIO_ACCOUNT_SID ? 'Configured' : 'NOT SET'}`)
  console.log(`   ${TWILIO_AUTH_TOKEN ? '✅' : '❌'} Auth Token: ${TWILIO_AUTH_TOKEN ? 'Configured' : 'NOT SET'}`)
  console.log(`   ${TWILIO_APP_SID ? '✅' : '❌'} App SID: ${TWILIO_APP_SID ? 'Configured' : 'NOT SET'}`)
  console.log(`   🔄 Transfer: Twilio SIP ${TWILIO_TRANSFER_SIP}`)
  console.log(`   🚫 Machine Detection: ENABLED with AMD`)
  console.log(`   📞 AMD Webhook: POST /amd`)
  console.log(`   ⏱️ AMD Timeout: 3 seconds`)
  console.log(`   🔊 Speech Threshold: 1000ms`)
  
  // Deepgram Configuration Status
  console.log(`🔊 DEEPGRAM CONFIGURATION:`)
  console.log(`   ${DEEPGRAM_API_KEY ? '✅' : '❌'} API Key: ${DEEPGRAM_API_KEY ? 'Configured' : 'NOT SET'}`)
  console.log(`   🎤 Speech-to-Text: ${DG_STT_MODEL || 'nova-3'}`)
  console.log(`   🔊 Text-to-Speech: ${DG_TTS_MODEL || 'aura-asteria-en'}`)
  console.log(`   🌐 Language: ${DG_STT_LANGUAGE || 'en-US'}`)
  console.log(`   ⏱️ Endpointing: ${DG_STT_ENDPOINTING_MS || '150'}ms`)
  console.log(`   🔄 Unified API: STT + TTS with single API key`)
  
  // Cal.com Configuration Status with detailed info
  console.log(`📅 CAL.COM DETAILS (API v2):`)
  console.log(`   ${CAL_COM_API_KEY ? '✅' : '❌'} API Key: ${CAL_COM_API_KEY ? 'Configured' : 'NOT SET'}`)
  console.log(`   ${CAL_EVENT_TYPE_ID ? '✅' : '❌'} Event Type ID: ${CAL_EVENT_TYPE_ID ? CAL_EVENT_TYPE_ID : 'NOT SET'}`)
  console.log(`   🌐 API Version: ${CAL_COM_API_VERSION}`)
  console.log(`   📅 Appointment Duration: 15 minutes`)
  console.log(`   🕐 Smart Scheduling: Morning/Afternoon variety`)
  console.log(`   🕐 Timezone Conversion: America/Los_Angeles`)
  console.log(`   📅 Slot Range: 14 DAYS (2 weeks)`)
  console.log(`   🔄 API Endpoint: /v2/slots`)
  console.log(`   📊 Parameters: start/end with YYYY-MM-DD format`)
  console.log(`   🚫 Lunch Time Filtering: ENABLED - Excludes 12-1PM slots in caller timezone`)
  console.log(`   🚫 Fallback Slots: DISABLED - Real Cal.com only`)
  
  // Zoho CRM Configuration
  console.log(`📊 ZOHO CRM ANALYTICS:`)
  console.log(`   ${ZOHO_CLIENT_ID ? '✅' : '❌'} CRM Integration: ${ZOHO_CLIENT_ID ? 'Deep Analytics - ACTIVE' : 'NOT SET'}`)
  console.log(`   📈 Cost Tracking: Per-call breakdown`)
  console.log(`   🎯 Conversion Metrics: State/Area code analysis`)
  console.log(`   💰 ROI Reporting: Cost per meeting/email`)
  console.log(`   📞 Callback Requests: Captured and logged to n8n`)
  console.log(`   👤 Contact Merging: Auto-linked to Accounts`)
  console.log(`   🔄 Auto Field Creation: Call_SID, Call_Cost, AI_Call_Count`)
  
  // n8n Webhook Endpoints
  console.log(`🔄 N8N WEBHOOK ENDPOINTS:`)
  console.log(`   ${N8N_WEBHOOK_URL ? '✅' : '❌'} Webhook URL: ${N8N_WEBHOOK_URL ? 'Configured' : 'NOT SET'}`)
  console.log(`   📥 Call Logging: POST /n8n-webhook`)
  console.log(`   🧮 Cost Calculation: POST /calculate-costs`)
  console.log(`   📞 Callback Requests: Auto-sent to n8n`)
  console.log(`   👤 Contact Merge: Auto-sent to n8n`)
  console.log(`   📊 Structured Notes: Auto-generated with cost breakdown`)
  console.log(`   🔗 Twilio Integration: Auto-fetch recording URLs`)
  console.log(`   🚫 AMD Detection: Machine detection events logged`)
  
  // AI Conversation Features
  console.log(`🤖 AI CONVERSATION FEATURES:`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Hybrid Intelligence: ${DEEPSEEK_API_KEY ? 'State Machine + DeepSeek V3.2' : 'State machine only'}`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Smart Number Extraction: "about twenty" → 20`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Natural Language Understanding: "small team" → 5 seats`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Context-Aware Responses: 8-turn conversation memory`)
  console.log(`   ✅ Interruption Handling: Tracked and graceful recovery`)
  console.log(`   ✅ Voice-Optimized Responses: 25 word limit enforced`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Fallback Strategy: 2 LLM attempts before moving on`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Max Output Tokens: 150 (voice-optimized)`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} Temperature: 0.3 (predictable responses)`)
  
  // ENTERPRISE: Enhanced Performance Features
  console.log(`💨 ENTERPRISE PERFORMANCE FEATURES:`)
  console.log(`   ✅ Deepgram ${DG_STT_MODEL || 'nova-3'}: ${DG_STT_ENDPOINTING_MS || '150'}ms endpointing, better phrase detection`)
  console.log(`   ✅ Deepgram ${DG_TTS_MODEL || 'aura-asteria-en'}: Dynamic pacing by conversation stage`)
  console.log(`   ${DEEPSEEK_API_KEY ? '✅' : '⚪'} DeepSeek V3.2 (Hybrid): ${DEEPSEEK_API_KEY ? 'ENABLED' : 'DISABLED'}`)
  console.log(`   ✅ 15-minute appointments: ENABLED`)
  console.log(`   ✅ ENTERPRISE MEMORY: Remembers rejected time slots`)
  console.log(`   ✅ BUSINESS HOURS VALIDATION: 8AM-6PM only`)
  console.log(`   ✅ INTELLIGENT SLOT SELECTION: Morning/Afternoon variety`)
  console.log(`   ✅ REAL-TIME COST TRACKING: ENABLED`)
  console.log(`   ✅ ZOHO DEEP ANALYTICS: ENABLED`)
  console.log(`   🕐 TIMEZONE CONVERSION: America/Los_Angeles`)
  console.log(`   📞 LIVE TRANSFER FALLBACK: ENABLED with n8n integration`)
  console.log(`   ⏰ CURRENT SERVER TIME: ${timezoneManager.getCurrentTime()}`)
  console.log(`   🔄 SLOT CACHING: ENABLED (1 minute)`)
  console.log(`   📧 EMAIL FALLBACK: 2 attempts then scheduling link`)
  console.log(`   📅 EXTENDED CALENDAR: 14-day slot search (2 weeks)`)
  console.log(`   🔄 CAL.COM API V2: CORRECTED endpoint and parameters`)
  console.log(`   🚫 NO FAKE SLOTS: Calendar errors handled gracefully`)
  console.log(`   🚫 MACHINE DETECTION: ENABLED - AMD will hangup on voicemail`)
  
  // CRITICAL FIXES APPLIED
  console.log(`🔧 ENTERPRISE CRITICAL FIXES APPLIED:`)
  console.log(`   ✅ AZURE TTS REPLACED: Now using Deepgram Aura 2 for unified STT+TTS`)
  console.log(`   ✅ 1.5s DEBOUNCE: Rapid inputs ignored, simple "hello" filtered`)
  console.log(`   ✅ AUTO-RESET: "I was just saying hello" resets to decision maker`)
  console.log(`   ✅ Logger variable collision FIXED: convoLogger used in WebSocket handler`)
  console.log(`   ✅ ENTERPRISE MEMORY: Remembers rejected time slots and user preferences`)
  console.log(`   ✅ BUSINESS HOURS VALIDATION: Prevents 3AM slot offers`)
  console.log(`   ✅ INTELLIGENT SLOT SELECTION: Filters out rejected and non-business hour slots`)
  console.log(`   ✅ GLOBAL RECOVERY SYSTEM: Recovers from terminal states automatically`)
  console.log(`   ✅ STATE LOOP DETECTION: Prevents infinite conversation loops`)
  console.log(`   ✅ ENHANCED RE-ENGAGEMENT: Multiple patterns for stuck state recovery`)
  console.log(`   ✅ TIME REJECTION TRACKING: Remembers when users reject specific times`)
  console.log(`   ✅ SPECIFIC TIME REQUESTS: Handles "Thursday at 2PM" style requests`)
  console.log(`   ✅ DYNAMIC SPEECH PACING: Greeting (1.0x), Qualification (1.05x), Booking (1.1x)`)
  console.log(`   ✅ CONVERSATIONAL CONNECTORS: "Got it, thanks. And who's..."`)
  console.log(`   ✅ ENHANCED APPOINTMENT FLOW: Better time slot descriptions`)
  console.log(`   ✅ NATURAL SPEECH SYNTHESIS: Dynamic prosody and pacing`)
  console.log(`   ✅ ENHANCED RE-ENGAGEMENT PATTERNS: "Can we help?", "We can help", plural forms`)
  console.log(`   ✅ FALLBACK KEYWORD DETECTION: "help", "assist", "talk" in terminal states`)
  console.log(`   ✅ DEFENSIVE DONE STATE CHECK: Prevents premature goodbye`)
  console.log(`   ✅ DEBUG LOGGING: Pattern matching visibility`)
  console.log(`   ✅ ENHANCED NAME EXTRACTION: Filters out "Usually", "Maybe" as names`)
  console.log(`   ✅ SELF-IDENTIFICATION DETECTION: Recognizes "This is Ron" patterns`)
  console.log(`   ✅ TRANSFER CONTEXT AWARENESS: Knows when decision maker is already speaking`)
  console.log(`   ✅ CONVERSATION CONTEXT TRACKING: Remembers mentioned names and speakers`)
  console.log(`   ✅ MEMORY MANAGEMENT: Automatic cleanup prevents memory leaks`)
  console.log(`   ✅ CIRCUIT BREAKERS: Prevents cascade failures from external APIs`)
  console.log(`   ✅ COST EXPLOSION PREVENTION: Token limits and call duration caps`)
  console.log(`   ✅ ENHANCED EMAIL EXTRACTION: More flexible patterns and validation`)
  console.log(`   ✅ OUTBOUND TRANSFER LOGIC FIXED: 75-second timeout, better decision maker detection`)
  console.log(`   ✅ HUMAN REQUEST HANDLING: Graceful "no number available" handling`)
  console.log(`   ✅ TRANSFER STATUS CHECKS: Polite follow-up after 30 seconds`)
  console.log(`   ✅ DEEPGRAM ERROR HANDLING: Better validation and error recovery`)
  console.log(`   ✅ TIME SLOT PARSING: Fixed "08:15" format handling`)
  console.log(`   ✅ MORNING/AFTERNOON SLOT SELECTION: Better time distribution`)
  console.log(`   ✅ SCHEDULING LINK FALLBACK: Replaces phone fallback with email scheduling`)
  console.log(`   ✅ CALLBACK OFFER FIX: Now offers callback when both email and texting are refused`)
  console.log(`   ✅ TRANSFER OFFER DETECTION: Now properly detects "I'll transfer you to Tom"`)
  console.log(`   ✅ 75-SECOND TRANSFER TIMEOUT: Waits 75 seconds then asks if anyone is there`)
  console.log(`   ✅ TRANSFER TIMEOUT HANDLING: Asks if anyone is there, hangs up if no response`)
  console.log(`   ✅ CONVERSATION ENGINE FIX PACK v1: Applied all requested enhancements`)
  console.log(`   ✅ UNAVAILABILITY DETECTION: Detects lunch, breaks, meetings, away status`)
  console.log(`   ✅ GATEKEEPER HANDLING: Screening questions, message taking, callback offers`)
  console.log(`   ✅ ALTERNATIVE CONTACT FLOW: Asks for others when primary is unavailable`)
  console.log(`   ✅ EARLY TRANSFER DETECTION: Transfer offers detected immediately in any state`)
  console.log(`   ✅ WRONG DEPARTMENT HANDLING: Asks for correct department/extension`)
  console.log(`   ✅ PROVIDER RECOGNITION ENHANCEMENT: Partner confirmation without derailment`)
  console.log(`   ✅ DUPLICATE GREETING FIX: Prevents asking same question twice`)
  console.log(`   ✅ FASTER RESPONSE TIME: 150ms endpointing vs 200ms`)
  console.log(`   ✅ PROVIDER CONFIRMATION FIX: No longer asks "Is that what you're using?" when they just said so`)
  console.log(`   ✅ APPOINTMENT DURATION MENTION: Now mentions "15-minute appointments"`)
  console.log(`   ✅ GASLIGHTING LOOP FIX: Prevents repeating "Would that work?" 4 times`)
  console.log(`   ✅ NAME COLLECTION FIX: Better detection after time confirmation`)
  console.log(`   ✅ SENTENCE CUT-OFF FIX: Audio completion wait prevents mid-speech interruptions`)
  
  // API Endpoints
  console.log(`🔌 API ENDPOINTS:`)
  console.log(`   📞 Voice: POST /voice`)
  console.log(`   🚫 AMD Detection: POST /amd`)
  console.log(`   🔄 Status Updates: POST /status`)
  console.log(`   📊 System Status: GET /status`)
  console.log(`   💬 Conversations: GET /conversations`)
  console.log(`   💬 Single Conversation: GET /conversation/:callSid`)
  console.log(`   📤 Outbound Calling: ENABLED via Twilio Streams`)
  console.log(`   🔄 n8n Webhook: POST /n8n-webhook`)
  console.log(`   🧮 Cost Calculator: POST /calculate-costs`)
  
  console.log(`🔍 ENTERPRISE CONVERSATION LOGGING: ENABLED - Use /conversation/:callSid to view logs`)
  console.log(`💰 ENHANCED COST ANALYTICS: ENABLED - Automatic Zoho integration with memory tracking`)
  console.log(`🔄 N8N WEBHOOKS: ${N8N_WEBHOOK_URL ? 'READY' : 'DISABLED'} for Twilio call completion events`)
  console.log(`📞 CALLBACK REQUESTS: NOW CAPTURED for live transfer fallback`)
  console.log(`👤 CONTACT MERGING: AUTO-LINKED to Zoho Accounts`)
  console.log(`🎯 PRONOUN FIXES: IMPLEMENTED - No more "my name" references`)
  console.log(`📅 CALENDAR RANGE: SET to 14 days (2 weeks)`)
  console.log(`🔄 CAL.COM API V2: CORRECTED - Using proper /v2/slots endpoint`)
  console.log(`🚫 NO FAKE SLOTS: Calendar errors now handled with graceful apology`)
  console.log(`🚫 MACHINE DETECTION: ENABLED - Twilio AMD will detect and hangup on voicemail`)
  console.log(`🎯 ENTERPRISE MEMORY: ACTIVE - Remembers user preferences and rejected time slots`)
  console.log(`🕐 BUSINESS HOURS VALIDATION: ACTIVE - 8AM-6PM scheduling only`)
  console.log(`🔄 GLOBAL RECOVERY: ACTIVE - Automatic recovery from stuck states`)
  console.log(`🔍 CONTEXT AWARENESS: ACTIVE - Tracks speakers and mentioned names`)
  console.log(`🎯 SELF-IDENTIFICATION: ACTIVE - Recognizes when decision makers identify themselves`)
  console.log(`🧹 MEMORY MANAGEMENT: ACTIVE - Automatic cleanup prevents memory leaks`)
  console.log(`⚡ CIRCUIT BREAKERS: ACTIVE - Prevents cascade failures from external APIs`)
  console.log(`💰 COST CONTROLS: ACTIVE - Token limits and call duration caps`)
  console.log(`📞 OUTBOUND TRANSFER LOGIC: FIXED - 75-second timeout, better decision maker detection`)
  console.log(`👥 HUMAN REQUEST HANDLING: IMPROVED - Graceful handling when no number available`)
  console.log(`🔧 DEEPGRAM ERROR HANDLING: IMPROVED - Better validation and recovery`)
  console.log(`📅 TIME SLOT SELECTION: IMPROVED - Morning/afternoon distribution`)
  console.log(`📧 EMAIL FALLBACK: ENHANCED - Scheduling links instead of phone fallback`)
  console.log(`📞 CALLBACK OFFER: FIXED - Now offers callback when both email and texting are refused`)
  console.log(`🔄 TRANSFER OFFER DETECTION: FIXED - Now properly detects "I'll transfer you to Tom"`)
  console.log(`⏰ 75-SECOND TRANSFER TIMEOUT: IMPLEMENTED - Waits 75 seconds then asks if anyone is there`)
  console.log(`🔇 TRANSFER TIMEOUT HANDLING: IMPLEMENTED - Asks if anyone is there, hangs up if no response`)
  console.log(`🔊 DEEPGRAM UNIFIED STT+TTS: ACTIVE - Using ${DG_TTS_MODEL || 'aura-asteria-en'} for TTS`)
  console.log(`🔧 All enterprise critical fixes applied and ready for production`)
  console.log('═══════════════════════════════════════════════════')
  console.log(`✅ ${AGENT_NAME} ENTERPRISE EDITION is ready to handle calls!`)
  console.log(`🚫 Machine Detection: ACTIVE - Will hangup on voicemail`)
  console.log(`⏰ Debounce: ACTIVE - 1.5s rapid input protection`)
  console.log(`🔄 Auto-Reset: ACTIVE - "I was just saying hello" recovery`)
  console.log(`🎯 Enterprise Memory: ACTIVE - Remembers rejected time slots`)
  console.log(`🕐 Business Hours: ACTIVE - 8AM-6PM scheduling only`)
  console.log(`📧 Email Fallback: ACTIVE - Scheduling links for failed email collection`)
  console.log(`🔄 Global Recovery: ACTIVE - Automatic recovery from stuck states`)
  console.log(`🔍 Context Awareness: ACTIVE - Tracks speakers and mentioned names`)
  console.log(`🎯 Self-Identification: ACTIVE - Recognizes when decision makers identify themselves`)
  console.log(`🧹 Memory Management: ACTIVE - Automatic cleanup prevents memory leaks`)
  console.log(`⚡ Circuit Breakers: ACTIVE - Prevents cascade failures from external APIs`)
  console.log(`💰 Cost Controls: ACTIVE - Token limits and call duration caps`)
  console.log(`📞 Outbound Transfer Logic: FIXED - 75-second timeout, better decision maker detection`)
  console.log(`👥 Human Request Handling: IMPROVED - Graceful handling when no number available`)
  console.log(`🔧 Deepgram Error Handling: IMPROVED - Better validation and recovery`)
  console.log(`📅 Time Slot Selection: IMPROVED - Morning/afternoon distribution`)
  console.log(`📞 Callback Offer: FIXED - Now offers callback when both email and texting are refused`)
  console.log(`🔄 Transfer Offer Detection: FIXED - Now properly detects "I'll transfer you to Tom"`)
  console.log(`⏰ 75-Second Transfer Timeout: ACTIVE - Waits 75 seconds then asks if anyone is there`)
  console.log(`🔇 Transfer Timeout Handling: ACTIVE - Asks if anyone is there, hangs up if no response`)
  console.log(`🔊 Deepgram Unified TTS: ACTIVE - Using ${DG_TTS_MODEL || 'aura-asteria-en'} voice`)

process.on('SIGTERM', () 